package main

import (
	"banana/libraries/logger"
	"banana/libraries/lru"
	"banana/libraries/s3simple"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/klauspost/compress/zstd"
	"github.com/samalba/buse-go/buse"
)

type Config struct {
	BlockSize  int
	ServerUrl  string
	ServerType int // 0=A10 local, 1=A10 url, 2=Cloud?
	AuthCode   string
	Guid       string //used for http (non-cloud) restores

	Bucket    string
	AccessKey string
	SecretKey string
	Region    string

	Version    string // an epoch really, but we'll treat it like a string
	DevNum     int
	DevicePath string
	WorkDir    string
}

type DeviceHCL struct {
	device      *os.File
	blockSize   int
	totalSize   uint
	totalBlocks uint
	hcl         []string
	blockBase   string
	s3sess      s3.S3
	httpClient  http.Client
	lru         *lru.LRUCache
}

type Block struct {
	print string
	len   int
	data  []byte
}

var g_cacheBlock Block
var g_config Config

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s /dev/nbd0\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(2)
}

func loadConfig(configPath string) {

	fmt.Printf("Loading config file: %s\n", configPath)

	fileContent, err := os.ReadFile(configPath)
	if err != nil {
		fatal(fmt.Sprintf("Error reading file: %s", err))
	}
	//fmt.Printf("Raw JSon: %s", fileContent)

	err = json.Unmarshal(fileContent, &g_config)
	if err != nil {
		fatal(fmt.Sprintf("Error unmarshalling JSON: %s", err))
	}

	//fmt.Printf("Unmarshalled: %+v", g_config)

	// do some sanity checks here?
	if g_config.BlockSize > 8 || g_config.BlockSize < 2 || g_config.BlockSize%2 != 0 {
		fatal(fmt.Sprintf("Invalid block size provided: %d", g_config.BlockSize))
	}

	if g_config.WorkDir == "" {
		g_config.WorkDir = fmt.Sprintf("/tmp/devmount")
	}
	os.Mkdir(g_config.WorkDir, 0775)

}

func main() {

	var hclFilePath string
	var devicePath string

	deviceExp := &DeviceHCL{}
	flag.IntVar(&deviceExp.blockSize, "b", 0, "BlockSize of HCL ")
	flag.StringVar(&hclFilePath, "f", "", "HCL File source")
	flag.StringVar(&devicePath, "d", "", "Block Device (eg /dev/nbd0)")
	confFile := flag.String("c", "", "Config File")

	g_config.ServerType = 0
	g_config.DevicePath = devicePath

	flag.Usage = usage
	flag.Parse()

	logger.Init("../logs/app.log", "devmounter")

	if *confFile != "" {
		loadConfig(*confFile)
		deviceExp.blockSize = g_config.BlockSize
		if g_config.AuthCode != "" {
			g_config.ServerType = 1
			// we share a transport for connection pooling
			transport := &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     30 * time.Second,
				TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
			}
			deviceExp.httpClient = http.Client{Transport: transport}
		} else if g_config.AccessKey != "" {
			g_config.ServerType = 2
			deviceExp.s3sess = s3simple.Setup(g_config.AccessKey, g_config.SecretKey, g_config.ServerUrl, g_config.Region)
		}

	} else {
		if len(hclFilePath) == 0 || len(devicePath) == 0 || deviceExp.blockSize == 0 {
			usage()
		}
		g_config.DevicePath = devicePath
	}

	deviceExp.lru = lru.NewLRUCache(deviceExp.blockSize)

	//var err error
	// we need to grab the hcl file from the web/s3 if we're not local
	//
	//	hclPath is the source, hclOutFile is the dest

	hclOutFile := g_config.WorkDir + "/dev.hcl"
	hclPath := fmt.Sprintf("/%s/%d.hcl", g_config.Version, g_config.DevNum)
	if g_config.ServerType == 1 || g_config.ServerType == 2 {
		err := fetchHcl(hclPath, hclOutFile, deviceExp)
		if err != nil {
			msg := fmt.Sprintf("failed to get block file from S3: %v", err)
			fatal(msg)
		}
		hclFilePath = hclOutFile
	}

	deviceExp.blockSize = deviceExp.blockSize << 20
	g_cacheBlock.data = make([]byte, deviceExp.blockSize)

	//if g_config.ServerType == 0 {
	hclFile, err := os.ReadFile(hclFilePath)
	if err != nil {
		msg := fmt.Sprintf("Cannot open hcl source: %s\n", err)
		fmt.Printf(msg)
		logger.Error(msg)
		os.Exit(1)
	}

	// get the path to the blocks
	components := strings.Split(hclFilePath, "/")
	if g_config.ServerType == 0 {
		deviceExp.blockBase = "/" + filepath.Join(components[:5]...) + "/blocks/"
	}

	// Load the print list into memory
	raw := string(hclFile)
	var tmp []string

	tmp = strings.Split(raw, "\n")
	for _, str := range tmp {
		if str != "" {
			if str == "CODA" {
				continue
			}
			deviceExp.hcl = append(deviceExp.hcl, str)
			deviceExp.totalBlocks++
		}
	}
	logger.Info(fmt.Sprintf("Loaded hcl file %s", hclFilePath))

	startDevice(*deviceExp)
}

func startDevice(deviceExp DeviceHCL) {
	deviceExp.totalSize = deviceExp.totalBlocks * uint(deviceExp.blockSize)

	logger.Info(fmt.Sprintf("Found block (%d) path %s\n", deviceExp.blockSize, deviceExp.blockBase))
	logger.Info(fmt.Sprintf("Device size: %d\n", deviceExp.totalSize))
	logger.Info(fmt.Sprintf("Device path: %s\n", g_config.DevicePath))

	device, err := buse.CreateDevice(g_config.DevicePath, uint(deviceExp.totalSize), &deviceExp)

	if err != nil {
		fmt.Printf("Cannot create device: %s\n", err)
		os.Exit(1)
	}
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)
	go func() {
		if err := device.Connect(); err != nil {
			logger.Error(fmt.Sprintf("Buse device stopped with error: %s", err))
		} else {
			logger.Info(fmt.Sprintln("Buse device stopped gracefully."))
		}
	}()
	<-sig
	// Received SIGTERM, cleanup
	logger.Info(fmt.Sprintln("SIGINT, disconnecting..."))
	device.Disconnect()
}

// S3 hclPath /TS/0.hcl			(the base url and bucket are handled)
//
//	http hclPath /ts/0.hcl		(the base url will inlclude the /storageid/guid/)
func fetchHcl(hclPath string, hclOutFile string, d *DeviceHCL) error {
	var content []byte
	var err error

	logger.Info(fmt.Sprintf("Fetching HCL from : %s, to: %s, base: %s, bucket: %s", hclPath, hclOutFile, g_config.ServerUrl, g_config.Bucket))

	if g_config.ServerType == 2 {
		hclPath = g_config.Guid + hclPath
		content, err = s3simple.GetFile(hclPath, g_config.Bucket, &d.s3sess)
		if err != nil {
			return err
		}
	} else if g_config.ServerType == 1 {
		url := g_config.ServerUrl + hclPath

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			msg := fmt.Sprintf("Error creating request: %s", err)
			fatal(fmt.Sprintf(msg))
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("Accept-Encoding", "identity")
		req.Header.Set("Authorization", g_config.AuthCode)

		resp, err := d.httpClient.Do(req)
		if err != nil {
			msg := fmt.Sprintf("Error getting block: %s", err)
			fatal(msg)
		}
		defer resp.Body.Close()

		content, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			msg := fmt.Sprintf("Error getting block: %s", err)
			fatal(msg)
		}
	}
	err = os.WriteFile(hclOutFile, content, 0644)
	if err != nil {
		fmt.Println("Error writing to hclfile:", err)
		return err
	}

	return nil
}

func decompress(compressed []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd NewReader for decompress: %w", err)
	}
	defer decoder.Close()

	decompressed, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress data: %w", err)
	}

	return decompressed, nil
}

// p is a buffer we write into and len(p) is how much they want to read
// offset is the offset into the block device for the start of the read
func (d *DeviceHCL) ReadAt(p []byte, offset uint) error {

	// Calculate which block they want, and the offset into that block
	blockNum := int(offset / uint(d.blockSize))

	want := len(p)

	blockOffset := int64(offset % uint(d.blockSize)) // where inside the block to start

	if blockNum >= len(d.hcl) {
		return nil // we're done?
	}

	print := d.hcl[blockNum]
	if g_cacheBlock.print != print {
		fetchBlock(print, blockNum, d)
	}

	// Calculate how many bytes we can read from this block
	bytesToRead := min(want, g_cacheBlock.len-int(blockOffset))

	copy(p, g_cacheBlock.data[blockOffset:int(blockOffset)+bytesToRead]) // copy just the bytes the asked for, or that we have
	totalRead := bytesToRead
	log.Printf("ReadAt: [block# %d, BlockOff: %d, BlockSize: %d] Read %d [want: %d] diskOff: %d", blockNum, blockOffset, g_cacheBlock.len, totalRead, want, offset)

	leftOver := want - totalRead
	if leftOver > 0 {
		log.Printf("More bytes needed (%d) after initial read", leftOver)
		print := d.hcl[blockNum+1]
		if g_cacheBlock.print != print {
			fetchBlock(print, blockNum, d)
		}
		blockOffset = 0
		copy(p[totalRead:], g_cacheBlock.data[0:leftOver])
	}

	return nil
}

func fetchBlock(print string, blockNum int, d *DeviceHCL) error {

	logger.Debug(fmt.Sprintf("Fetching new block [%d]: %s", blockNum, print))

	var found = false
	if g_cacheBlock.data, found = d.lru.Get(print); found {
		logger.Debug(fmt.Sprintf("*Block %s found in cache*", print))
		return nil
	} else {
		logger.Debug(fmt.Sprintf("Block %s not in cache, must fetch it", print))
	}

	var content []byte
	var err error
	if g_config.ServerType == 0 {
		content, err = fetchBlockLocal(print, d)
	} else if g_config.ServerType == 1 {
		content, err = fetchBlockHttp(print, d)
	} else if g_config.ServerType == 2 {
		content, err = fetchBlockS3(print, d)
	}
	if err != nil {
		msg := fmt.Sprintf("Failed to fetch block %s: %v", print, err)
		fatal(msg)
		return nil
	}

	g_cacheBlock.data, err = decompress(content)
	if err != nil {
		msg := fmt.Sprintf("failed to decompress block %s: %v", print, err)
		fatal(msg)
		return nil
	}
	cerr := d.lru.Set(print, g_cacheBlock.data)
	if cerr != nil {
		logger.Error(fmt.Sprintf("Error setting lru: %s: %v", print, cerr))
	}

	g_cacheBlock.len = len(g_cacheBlock.data)
	g_cacheBlock.print = print

	hash := md5.Sum(g_cacheBlock.data) // get the print/md5
	hashString := hex.EncodeToString(hash[:])
	if print != hashString {
		msg := fmt.Sprintf("Block FAILED MD5 CHECK!  Loaded[%d]: %s vs calc: %s", blockNum, print, hashString)
		fatal(msg)
	}

	return nil
}
func fetchBlockLocal(print string, d *DeviceHCL) ([]byte, error) {
	blockFileName := d.blockBase + string(print[:3]+"/"+print)

	content, err := os.ReadFile(blockFileName)
	if err != nil {
		return nil, err
	}
	return content, nil
}

// S3 hclPath http://s3.com/GUID/TS/0.hcl
func fetchBlockS3(print string, d *DeviceHCL) ([]byte, error) {
	blockFileName := g_config.Guid + "/blocks/" + string(print[:3]+"/"+print)

	logger.Debug(fmt.Sprintf("fetching from S3: %s (%s)", blockFileName, print))

	content, err := s3simple.GetFile(blockFileName, g_config.Bucket, &d.s3sess)
	if err != nil {
		return nil, err
	}

	return content, nil
}

// Web http://127.0.0.1/data/1/GUID/blocks
func fetchBlockHttp(print string, d *DeviceHCL) ([]byte, error) {
	url := g_config.ServerUrl + "/blocks/" + string(print[:3]+"/"+print)

	//fmt.Printf("\nActual block GET: %s\n", url)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fatal(fmt.Sprintf("Error creating request: %s", err))
	}
	//req.Header.Set("Authorization", g_config.AuthCode)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Authorization", g_config.AuthCode)

	resp, err := d.httpClient.Do(req)
	if err != nil {
		fatal(fmt.Sprintf("Error getting block: %s", err))
	}
	defer resp.Body.Close()

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fatal(fmt.Sprintf("Error getting block: %s", err))
		return nil, err
	}

	return content, nil
}

func (d *DeviceHCL) WriteAt(p []byte, off uint) error {
	//	copy(d.dataset[off:], p)
	log.Printf("[DeviceHCL] WRITE offset:%d len:%d\n", off, len(p))
	return nil
}

func (d *DeviceHCL) Disconnect() {
	logger.Debug(fmt.Sprintln("[DeviceHCL] DISCONNECT"))
}

func (d *DeviceHCL) Flush() error {
	logger.Debug(fmt.Sprintln("[DeviceHCL] Flush"))
	return nil
}

func (d *DeviceHCL) Trim(off, length uint) error {
	logger.Debug(fmt.Sprintf("[DeviceHCL] TRIM offset:%d len:%d\n", off, length))
	return nil
}

func fatal(msg string) {
	logger.Error(msg)
	fmt.Println(msg)
	//log("Fatal error: " + msg)

	os.Exit(1)
}
