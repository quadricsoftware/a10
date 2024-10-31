package main

import (
	"bytes"
	"compress/zlib"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"

	//	"github.com/quadricsoftware/banana/s3simple"
	"banana/libraries/s3simple"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"os/exec"

	"github.com/klauspost/compress/zstd"
	"github.com/shopspring/decimal"
)

type LogLevel int

const (
	ERROR   LogLevel = 3
	WARNING LogLevel = 4
	INFO    LogLevel = 6
	DEBUG   LogLevel = 7
)

type Chunk struct {
	Number int
	Data   []byte
	MD5    string
}

type Stats struct {
	TotalBytes            int64
	StartTime             int
	SecondsTotal          string
	BytesRead             int64
	BytesSent             int64
	BytesSentCompressed   int64
	NumBlocksSent         int32
	NumBlocksSkippedLocal int32
	NumBlocksSkippedCBT   int32
	NumBlocksSkippedHCA   int32
	NumBlocksProcessed    int32
	NumBlocks             int32
	Progress              string
	Timestamp             int
}

type Config struct {
	JobID       int
	JobType     int // 0=backup, 1=restore, 2=scan?
	WorkDir     string
	XclFiles    []string
	HcaFiles    []string
	Timestamp   int
	BlockSize   int
	Concurrency int
	Sources     []string `json:"Sources"`
	Targets     []string `json:"Targets"`
	ServerUrl   string
	ServerType  int // 0=A10 local, 1=A10 url, 2=Cloud?
	AuthCode    string
	Bucket      string
	AccessKey   string
	SecretKey   string
	Region      string
	Guid        string
	NumWorkers  int
	LogLevel    LogLevel
}

var logMutex = sync.Mutex{}

// var statMutex = sync.Mutex{}
var g_stats Stats
var g_config Config

var g_httpClient http.Client

var g_lastStat = time.Now()
var g_start = time.Now()
var g_sentChunks sync.Map // global sync map for avoiding dup block sends

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s <source_file> <url/local QDS path> [block size MB] [num http clients]\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(1)
}

func loadConfig(configPath string) {

	fmt.Printf("Loading config file: %s\n", configPath)

	dat, err := os.ReadFile(configPath)
	if err != nil {
		fatal(fmt.Sprintf("Error reading file: %s", err))
		return
	}
	parseConfig(dat)
}

func parseConfig(confContent []byte) {

	g_config.LogLevel = INFO

	err := json.Unmarshal(confContent, &g_config)
	if err != nil {
		fatal(fmt.Sprintf("Error unmarshalling JSON: %s", err))
		return
	}

	log(DEBUG, fmt.Sprintf("Unmarshalled: %+v", g_config))

	// do some sanity checks here?
	if g_config.BlockSize > 8 || g_config.BlockSize < 2 || g_config.BlockSize%2 != 0 {
		fatal(fmt.Sprintf("Invalid block size provided: %d", g_config.BlockSize))
	}

	// scans don't have servers
	if g_config.JobType != 0 && g_config.JobType != 1 {
		fatal("Config based jobs must be backup (0) or restore (1)")
	}

	if isURL(g_config.ServerUrl) {
		log(DEBUG, fmt.Sprintf("Found URL: %s", g_config.ServerUrl))
		if g_config.ServerType != 2 {
			g_config.ServerType = 1 // don't overwrite their cloud config setting (eg 2)
		}
	} else if isLocalPath(g_config.ServerUrl) {
		g_config.ServerUrl = strings.TrimRight(g_config.ServerUrl, "/")
		log(DEBUG, fmt.Sprintf("Found local path: %s", g_config.ServerUrl))
		g_config.ServerType = 1
	} else {
		fatal(fmt.Sprintf("URL or local path not valid QDS source: %s\n", g_config.ServerUrl))
	}

	if g_config.Timestamp == 0 {
		g_config.Timestamp = int(time.Now().UTC().Unix())
	}
	if g_config.WorkDir == "" {
		g_config.WorkDir = fmt.Sprintf("/tmp/job.%d", g_config.JobID)
	}
	os.Mkdir(g_config.WorkDir, 0775)

	//fmt.Printf("Finished Loading config file: %s\n", fileContent)

}

func main() {
	g_stats.StartTime = int(time.Now().UTC().Unix())

	g_stats.NumBlocks = 0
	g_config.BlockSize = 4
	g_config.NumWorkers = 4

	url := ""
	sourceFile := ""
	targetPath := ""

	confFile := flag.String("c", "", "Config File")
	jsonArg := flag.String("j", "", "Config json string")

	scanFile := flag.String("s", "", "Perform Scan on device")

	flag.IntVar(&g_config.NumWorkers, "w", 4, "The number of coroutines")
	flag.IntVar(&g_config.BlockSize, "k", 4, "Block size in MB (default 4))")

	flag.StringVar(&url, "server", "", "Server Url/Path (backup)")
	flag.StringVar(&sourceFile, "src", "", "Data source path (backup eg. /dev/xvdc)")
	flag.StringVar(&targetPath, "o", "", "Outfile for restore (file or device)")

	flag.Usage = usage
	flag.Parse()

	if *jsonArg != "" {
		parseConfig([]byte(*jsonArg)) // load the config from the json string arg
	} else if *confFile != "" {
		loadConfig(*confFile) // load the config from a file
	} else {
		if *scanFile == "" {
			fmt.Println("Config file require for everything but a scan (-s)")
			usage()
		}

		doScan(*scanFile)
		return
	}

	g_stats.Timestamp = g_config.Timestamp
	g_config.BlockSize = g_config.BlockSize << 20 // convert blocksize to bytes

	// scans are easy
	if g_config.JobType == 2 {
		doScan(sourceFile)
	} else if g_config.JobType == 0 {
		doMunge()
	} else if g_config.JobType == 1 {
		doMerge()
	}

	os.Exit(0)
}

func doScan(sourceFile string) {
	if sourceFile == "" {
		fatal("Missing required argument: -src (source file)\n")
	}
	if _, err := os.Stat(sourceFile); errors.Is(err, os.ErrNotExist) {
		fatal(fmt.Sprintf("File does not exist: %s\n", sourceFile))
	}

	file, err := os.Open(sourceFile)
	if err != nil {
		fatal(fmt.Sprintf("Error opening file: %s, Error: %s", sourceFile, err))
	}
	defer file.Close()
	pos, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		fatal(fmt.Sprintf("error seeking to end of %s: %s", sourceFile, err))
	}
	file.Seek(0, io.SeekStart)
	g_stats.NumBlocks = int32(pos / int64(g_config.BlockSize))

	log(INFO, fmt.Sprintf("Beginning scan of %s. BlockSize: %d", sourceFile, g_config.BlockSize))
	buffer := make([]byte, g_config.BlockSize)

	for {
		bytesRead, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			fatal(fmt.Sprintf("Error reading source data:%s", err))
		} else if bytesRead == 0 {
			break
		}
		g_stats.BytesRead += int64(bytesRead)
		chunk := buffer[:bytesRead]

		hash := md5.Sum(chunk) // get the print/md5
		hashString := hex.EncodeToString(hash[:])

		fmt.Printf("%s\n", hashString)
		atomic.AddInt32(&g_stats.NumBlocksProcessed, 1)
	}

	log(INFO, "Finished processing data")

	writeStats()
}

// This loops over all the targets
// We process each restore device concurrently -- NB the get/write for each disk is currently single file
func doMerge() {

	var wg sync.WaitGroup
	wg.Add(len(g_config.Targets))

	// loop over all the devices to restore and work on them concurrently
	i := 0
	for _, disk := range g_config.Targets {
		go mergeDisk(disk, i, &wg)
		i++
	}

	wg.Wait()
	fmt.Println("Done with doMerge")
}

func mergeDisk(destFile string, diskNum int, wg *sync.WaitGroup) {
	defer wg.Done()

	// Source possibilites:
	//	http://127.0.0.1/data/storageid/vmguid/ts/deviceNum		<-- will stream a complete (compressed) disk image
	//	/mnt/storage/1/GUID/TS/0.hcl
	//	NOT USED!!! Non-cloud restores always use the Stream approach  http://127.0.0.1/data/1/GUID/TS/0.hcl	// <-- auth is in config
	//	http://s3.com/GUID/TS/0.hcl				// <-- bucket etc is in config

	// Here we decide how to get the data to restore
	target := openMungeDest(destFile)
	defer target.Close()
	if g_config.ServerType == 1 {
		// "normal" restore
		mergeDiskFromStream(g_config.Sources[diskNum], target)
	} else if g_config.ServerType == 2 {
		// cloud
		mergeDiskFromCloud(g_config.Sources[diskNum], target)
	} else if g_config.ServerType == 0 {
		// filesystem.  not implemented
	}
}

// We simply open whatever the target file/device is for writing
// If a file exists we will truncate and overwrite it
// If no file exists, we will make one
// If it is a device, we start from byte 0
func openMungeDest(destFile string) *os.File {
	var flags int
	// setup our OpenFile flags according to the situation (new file, existing file, block device)
	if _, err := os.Stat(destFile); errors.Is(err, os.ErrNotExist) {
		fmt.Printf("Creating new file: %s\n", destFile)
		flags = os.O_CREATE | os.O_TRUNC | os.O_WRONLY
	} else {
		fileInfo, _ := os.Stat(destFile)
		mode := fileInfo.Mode()
		if mode.IsRegular() {
			fmt.Printf("Overwriting existing file! %s\n", destFile)
			flags = os.O_CREATE | os.O_TRUNC | os.O_WRONLY // Create/overwrite and open for writing
		} else if mode&os.ModeDevice != 0 {
			fmt.Printf("%s is a device file\n", destFile)
			flags = os.O_RDWR // Open for reading and writing
		}
	}
	log(INFO, fmt.Sprintf("Opening file for writing %s", destFile))

	file, err := os.OpenFile(destFile, flags, 0644)
	if err != nil {
		fatal(fmt.Sprintf("Error opening output device/file: %s, Error: %s", destFile, err))
	}
	return file
	// Now the target file is open, and ready for writing.
}

// Receives all data from a single, zstd compressed stream, decompresses it and writes linearly to a provided file/device
// This is intended to consume pre-prepared output from our server via http only
// It is not aware of any underlying structures (hcls, blocks, etc)
//
//	The URL should be a full path to GET a raw device -- it expects the stream to be compressed with zstd
//
//	TODO: Add some form of progress
func mergeDiskFromStream(url string, file *os.File) {

	log(INFO, fmt.Sprintf("Performing http get on %s", g_config.ServerUrl))

	// Perform the single GET request with authentication headers
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fatal(fmt.Sprintf("Error creating request: %s", err))
	}
	req.Header.Set("Authorization", g_config.AuthCode)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fatal(fmt.Sprintf("Error sending request: %s", err))

	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != 208 {
		fatal(fmt.Sprintf("Server returned non-200 status: %v", resp.Status))
	}

	// This won't work!  The server doesn't know the stream length apriori b/c of compression
	totalSize := resp.ContentLength          // this is always 0
	atomic.AddInt32(&g_stats.NumBlocks, 100) // FAKE!  We need to know the size apriori

	// Now we just pull down the bytes, decompressing the stream as we go
	//
	var dbytes int64
	// Check if the response is zstd compressed.  Might we use other options?
	if resp.Header.Get("Content-Encoding") == "zstd" {
		log(DEBUG, fmt.Sprintln("Received a compressed stream"))
		reader, err := zstd.NewReader(resp.Body)
		if err != nil {
			panic(err)
		}
		defer reader.Close()
		buf := make([]byte, 1024*1024*2) // 1MB buffer

		for {
			n, err := reader.Read(buf)
			if err != nil && err != io.EOF {
				panic(err)
			}
			if n == 0 {
				break
			}
			_, err = file.Write(buf[:n])
			if err != nil {
				panic(err)
			}
			dbytes += int64(n)

			atomic.AddInt32(&g_stats.NumBlocksProcessed, 1)
			if totalSize > 0 {
				progress := float64(dbytes) / float64(totalSize) * 100
				fmt.Printf("Decompression progress: %.2f%%\n", progress)
			} else {
				fmt.Printf("Wrote %d bytes\r", dbytes)
			}
		}
	}
	log(INFO, fmt.Sprintf("Downloaded a total of %d bytes", dbytes))

}

// We take a full path to an hcl, and a pre-opened file handle, and go to town
func mergeDiskFromCloud(hclPath string, target *os.File) {
	// hclPath should look somewhat like this:
	// VMGuid/1719028377/0.hcl
	sess := s3simple.Setup(g_config.AccessKey, g_config.SecretKey, g_config.ServerUrl, g_config.Region)

	dat, err := s3simple.GetFile(hclPath, g_config.Bucket, &sess)
	if err != nil {
		fmt.Printf("Error downloading HCL (%s): %v", hclPath, err)
	}
	clear, _ := decompress(dat)
	fmt.Printf("Got HCL from S3:\n|%s|\n", clear)

	contentStr := strings.TrimRight(string(clear), " \t\n\r")
	if !strings.HasSuffix(contentStr, "CODA") {
		fatal(fmt.Sprintf("\nHCL is missing CODA: %s", hclPath))
	}
	lines := strings.Split(contentStr, "\n")
	if len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "CODA" {
		lines = lines[:len(lines)-1]
	}
	var prints []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			// we should now fully qualify the print for the restore
			// eg. VMGuid/blocks/abc/block
			trimmed = fmt.Sprintf("%s/blocks/%s/%s", g_config.Guid, trimmed[:3], trimmed)
			prints = append(prints, trimmed)
		}
	}

	// this is required for progress calculation
	atomic.AddInt32(&g_stats.NumBlocks, int32(len(prints)))

	// For now this is linear and slow. We should make a goroutine and add some channels to speed things up
	i := 0
	for _, print := range prints {
		fmt.Printf("Getting block %s\n", print)
		dat, err := s3simple.GetFile(print, g_config.Bucket, &sess)
		if err != nil {
			fatal(fmt.Sprintf("Error downloading block (%s): %v", print, err))
		}
		blob, _ := decompress(dat)
		_, err = target.Write(blob)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Wrote block %s\n", print)
		atomic.AddInt32(&g_stats.NumBlocksProcessed, 1)
		i++
	}
}

/////////////////////////////////////////////////////////////////////////////// End Merge ////////////////////////////

//	Munge notes
//
// file is the source (device, volume, or whatever) and is already open
// sourceFile is just the string to the path of the file
// numworkers is obv
// url can be the http:// url of the server, or can be a local path to the root of the QDS
func doMunge() {

	g_stats.Progress = "0"
	fmt.Println("doing munge")

	if isURL(g_config.ServerUrl) {
		log(DEBUG, fmt.Sprintf("Found URL: %s", g_config.ServerUrl))
	} else if isLocalPath(g_config.ServerUrl) {
		g_config.ServerUrl = strings.TrimRight(g_config.ServerUrl, "/")
		log(DEBUG, fmt.Sprintf("Found local path: %s", g_config.ServerUrl))
		g_config.ServerType = 0
	} else {
		fmt.Printf("URL or local path not valid QDS source: %s\n", g_config.ServerUrl)
		os.Exit(1)
	}
	log(INFO, fmt.Sprintf("Beginning work. Server: %s, BlockSize: %d, Workers: %d", g_config.ServerUrl, g_config.BlockSize, g_config.NumWorkers))

	fmt.Println("starting munge")

	chunkChan := make(chan Chunk, g_config.NumWorkers*4) // 2nd arg is how many 'buffers' the channel has
	wg := sync.WaitGroup{}
	wg.Add(g_config.NumWorkers)

	// we share a transport for connection pooling
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     30 * time.Second,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
	}
	g_httpClient = http.Client{Transport: transport}

	if g_config.ServerType == 0 {
		fmt.Println("Local server path")
		for i := 0; i < g_config.NumWorkers; i++ {
			go localWorker(g_config.ServerUrl, chunkChan, &wg) //  we work on a local fs path to a QDS
		}
	} else if g_config.ServerType == 1 {
		fmt.Println("http server path")
		for i := 0; i < g_config.NumWorkers; i++ {
			go webWorker(&g_httpClient, g_config.ServerUrl, chunkChan, &wg) // we work against an http listener
		}
	} else if g_config.ServerType == 2 {
		fmt.Println("Cloud path ")
		for i := 0; i < g_config.NumWorkers; i++ {
			go webWorker(&g_httpClient, g_config.ServerUrl, chunkChan, &wg) // we work against an http listener
		}
	}

	wgm := sync.WaitGroup{}
	wgm.Add(len(g_config.Sources))

	i := 0
	for _, disk := range g_config.Sources {
		go mungeDisk(disk, i, chunkChan, &wgm)
		i++
	}
	wgm.Wait()

	close(chunkChan)
	wg.Wait()
	sendAssets(&g_httpClient) // this sends the HCL, log, and anything else we want to store per disk, per backup
	fmt.Println("Done with doMunge")
}

func mungeDisk(sourceFile string, diskNum int, chunkChan chan Chunk, wgm *sync.WaitGroup) {
	defer wgm.Done()
	if _, err := os.Stat(sourceFile); errors.Is(err, os.ErrNotExist) {
		fatal(fmt.Sprintf("File does not exist: %s\n", sourceFile))
	}

	file, err := os.Open(sourceFile)
	if err != nil {
		fatal(fmt.Sprintf("Error opening file: %s, Error: %s", sourceFile, err))
	}
	defer file.Close()

	var stats = new(Stats)
	var deviceSize int64 = 0

	if runtime.GOOS == "windows" {
		log(INFO, "Detected Windows platform.  Device size function is unavailable")

		deviceSize, err = getDiskSizeWin(diskNum)
		if err != nil {
			fatal(fmt.Sprintf("error getting win size of device %s: %s", sourceFile, err))
		}
		if deviceSize == 0 {
			deviceSize = 1
		}

	} else {
		log(INFO, "Detected Linux/unix platform")
		pos, err := file.Seek(0, io.SeekEnd)
		if err != nil {
			fatal(fmt.Sprintf("error seeking to end of %s: %s", sourceFile, err))
		}
		deviceSize = pos
		file.Seek(0, io.SeekStart)

	}
	log(DEBUG, fmt.Sprintf("Disk %d (%s) is %d bytes.", diskNum, sourceFile, deviceSize))

	stats.NumBlocks = int32(deviceSize / int64(g_config.BlockSize))
	if deviceSize%int64(g_config.BlockSize) != 0 {
		stats.NumBlocks++
	}

	log(INFO, fmt.Sprintf("Beginning work on disk %d (%s). Size: %d, NumBlocks: %d", diskNum, sourceFile, deviceSize, stats.NumBlocks))

	//stats
	stats.TotalBytes = deviceSize
	atomic.AddInt64(&g_stats.TotalBytes, int64(deviceSize))
	atomic.AddInt32(&g_stats.NumBlocks, stats.NumBlocks)

	var hcl []string
	var xcl []string
	var isDiff = false
	var isCBT = false
	if len(g_config.HcaFiles) > diskNum {
		log(DEBUG, fmt.Sprintf("Attempting to load HCA for disk %d, %s", diskNum, g_config.HcaFiles[diskNum]))
		hcl = loadHCA(g_config.HcaFiles[diskNum])
		if len(hcl) == int(stats.NumBlocks) {
			isDiff = true
			log(INFO, fmt.Sprintf("Loaded HCA for disk %d, %s  NumBlocks: %d", diskNum, g_config.HcaFiles[diskNum], stats.NumBlocks))
		} else if len(hcl) > 0 {
			log(WARNING, fmt.Sprintf("Mismatch in HCA length: expected %d lines, found %d lines.  Performing baseline\n", stats.NumBlocks, len(hcl)))
		}
	} else {
		log(WARNING, fmt.Sprintf("No HCA provided for disk %d", diskNum))
	}
	if len(g_config.XclFiles) > diskNum {
		if isDiff == false {
			log(INFO, fmt.Sprintf("CBT disabled! XCL provided but differential HCA failed/missing "))
		} else {
			xcl = loadCBT(g_config.XclFiles[diskNum])
			if len(xcl) == int(stats.NumBlocks) {
				isCBT = true
				log(INFO, fmt.Sprintf("CBT enabled. Loaded XCL for disk %d, %s", diskNum, g_config.XclFiles[diskNum]))
			} else if len(xcl) > 0 {
				log(WARNING, fmt.Sprintf("Mismatch in CBT length: expected %d lines, found %d lines.  Skipping CBT\n", stats.NumBlocks, len(xcl)))
			}
		}
	} else {
		log(INFO, fmt.Sprintf("No CBT provided for disk %d", diskNum))
	}

	buffer := make([]byte, g_config.BlockSize)
	chunkNumber := 0

	for {
		writeStats()

		if isCBT && chunkNumber < len(xcl) && xcl[chunkNumber] == "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF" {
			_, err := file.Seek(int64(g_config.BlockSize), io.SeekCurrent) // seek forward a block
			if err != nil {
				fatal(fmt.Sprintf("Error seeking file:%s", err))
			}
			log(DEBUG, fmt.Sprintf("CBT says skip block %d\n", chunkNumber))
			chunkNumber++
			atomic.AddInt32(&g_stats.NumBlocksSkippedCBT, 1)
			atomic.AddInt32(&g_stats.NumBlocksProcessed, 1)
			continue
		}
		if chunkNumber == int(stats.NumBlocks) {
			log(INFO, fmt.Sprintf("We've read all the blocks (%d vs %d)", chunkNumber, stats.NumBlocks))
			break
		}

		bytesRead, err := file.Read(buffer) // read the next block
		if err != nil && err != io.EOF {
			fatal(fmt.Sprintf("Error reading source data at block %d, bytes: %d, total bytes: %d: DeviceSize: %d, err: %s", chunkNumber, bytesRead, g_stats.BytesRead, deviceSize, err))
		}
		if bytesRead == 0 {
			break
		}
		//		stats.BytesRead += int64(bytesRead)
		atomic.AddInt64(&g_stats.BytesRead, int64(bytesRead))
		atomic.AddInt32(&g_stats.NumBlocksProcessed, 1)
		chunk := buffer[:bytesRead]
		hash := md5.Sum(chunk) // get the print/md5
		hashString := hex.EncodeToString(hash[:])

		// Skip blocks the previous version already has
		if isDiff && chunkNumber < len(hcl) && hcl[chunkNumber] == hashString {
			log(DEBUG, fmt.Sprintf("Block %d matches HCL, skipping", chunkNumber))
			chunkNumber++
			atomic.AddInt32(&g_stats.NumBlocksSkippedHCA, 1)
			continue
		}

		// update the HCL now...
		if chunkNumber < len(hcl) {
			hcl[chunkNumber] = hashString // Working from an HCA
		} else {
			hcl = append(hcl, hashString) // new HCLs
		}

		// skip sending blocks we already sent in this run
		// NOTE - this happens AFTER CBT and HCA checks, so it is a new, different block and must be recorded in the HCL
		if _, exists := g_sentChunks.Load(hashString); exists {
			log(DEBUG, fmt.Sprintf("Block %d  [%s] already sent, skipping", chunkNumber, hashString))
			chunkNumber++

			atomic.AddInt32(&g_stats.NumBlocksSkippedLocal, 1)

			continue
		}

		// this is the channel for the workers.  they pull from here to do things
		chunkChan <- Chunk{
			Number: chunkNumber,
			Data:   append([]byte(nil), chunk...),
			MD5:    hashString,
		}

		g_sentChunks.Store(hashString, true)
		chunkNumber++

		atomic.AddInt32(&g_stats.NumBlocksSent, 1)
	}

	log(INFO, "Finished processing data")

	// complete the HCL by adding the CODA
	hcl = append(hcl, "CODA")
	hclRaw := strings.Join(hcl, "\n")
	hclRaw = strings.TrimSuffix(hclRaw, "\n")
	hclFileNum := fmt.Sprintf("%d.hcl", diskNum)

	if g_config.ServerType == 0 {
		// local FS write
		hclFileName := fmt.Sprintf("%s/%d/%s", g_config.ServerUrl, g_config.Timestamp, hclFileNum)
		os.MkdirAll(filepath.Dir(hclFileName), 0755)
		err1 := os.WriteFile(hclFileName, []byte(hclRaw), 0644)
		if err1 != nil {
			fatal(fmt.Sprintf("Error writing file (%s): %s", hclFileName, err1))
		}
	} else if g_config.ServerType == 1 {
		// A10 http write
		hclFileName := fmt.Sprintf("%s/file/%d/%s", g_config.ServerUrl, g_config.Timestamp, hclFileNum)
		err := putFile(&g_httpClient, hclFileName, hclRaw)
		if err != nil {
			fatal(fmt.Sprintf("Error uploading HCL! Err: %s", err))
		}
	} else if g_config.ServerType == 2 {
		fileName := fmt.Sprintf("%d/%s", g_config.Timestamp, hclFileNum)
		putFileCloud(fileName, hclRaw)
	}

	end := time.Now()
	elapsed := end.Sub(g_start)

	bitsTransferred := float64(g_stats.BytesSent) * 8
	mbitsTransferred := bitsTransferred / 1000000
	dataRate := mbitsTransferred / elapsed.Seconds()

	bitsRead := float64(g_stats.BytesRead) * 8
	mbitsRead := bitsRead / 1000000
	readRate := mbitsRead / elapsed.Seconds()
	log(INFO, fmt.Sprintf("%d/%d blocks processed", g_stats.NumBlocksProcessed, g_stats.NumBlocks))
	log(INFO, fmt.Sprintf("Read %d/Sent: %d in %.2f seconds", g_stats.BytesRead, g_stats.BytesSent, elapsed.Seconds()))
	log(INFO, fmt.Sprintf("Local read speed: %.2f Mb/s, Network transfer rate: %.2f Mb/s", readRate, dataRate))

	writeStats() // just update the stats.json

}

func localWorker(localPath string, chunkChan chan Chunk, wg *sync.WaitGroup) {
	defer wg.Done()

	for chunk := range chunkChan {

		//log(fmt.Sprintf("Block %d  [%s] ", chunk.Number, chunk.MD5))

		blockFileName := localPath + "/blocks/" + string(chunk.MD5[:3]+"/"+chunk.MD5)

		_, err := os.Stat(blockFileName)
		if err == nil {
			log(DEBUG, fmt.Sprintf("Skipped writing existing Block %d: %s, ", chunk.Number, blockFileName))

		} else {

			b, _ := compress(chunk.Data[:]) // compress the block (pass by slice)
			atomic.AddInt64(&g_stats.BytesSentCompressed, int64(b.Len()))
			atomic.AddInt64(&g_stats.BytesSent, int64(g_config.BlockSize))

			err1 := os.WriteFile(blockFileName, b.Bytes(), 0644) // create or overwrite file
			if err1 != nil {
				fatal(fmt.Sprintf("Error writing block %d (%s): %s", chunk.Number, blockFileName, err))
			}

			writeStats()
			//log(fmt.Sprintf("Block %d print: %s, Size: %d", chunk.Number, chunk.MD5, len(chunk.Data)))
		}
	}
}

func webWorker(client *http.Client, url string, chunkChan chan Chunk, wg *sync.WaitGroup) {
	defer wg.Done()

	sess := s3simple.Setup(g_config.AccessKey, g_config.SecretKey, g_config.ServerUrl, g_config.Region)

	for chunk := range chunkChan {
		b, _ := compress(chunk.Data[:]) // compress the block (pass by slice)

		atomic.AddInt64(&g_stats.BytesSentCompressed, int64(b.Len()))
		atomic.AddInt64(&g_stats.BytesSent, int64(len(chunk.Data)))

		log(DEBUG, fmt.Sprintf("Block %d print: %s, Size: %d, Comp: %d", chunk.Number, chunk.MD5, len(chunk.Data), b.Len()))

		if g_config.ServerType == 1 {
			err := putBlock(client, url+"/block/"+chunk.MD5, b.Bytes())
			if err != nil {
				fatal(fmt.Sprintf("Error uploading block %d: %s", chunk.Number, err))
			}
		} else if g_config.ServerType == 2 {
			block := fmt.Sprintf("%s/blocks/%s/%s", g_config.Guid, chunk.MD5[:3], chunk.MD5)
			log(DEBUG, fmt.Sprintf("About to put %s to bucket: %s", block, g_config.Bucket))
			s3simple.PutBlock(block, b.Bytes(), g_config.Bucket, &sess, false)
		}

		writeStats()
	}
}

func putBlock(client *http.Client, url string, chunk []byte) error {
	req, err := http.NewRequest("PUT", url, bytes.NewReader(chunk))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Authorization", g_config.AuthCode)
	//	req.Header.Set("Content-Encoding", "gzip")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body) // <--- this is vital for windows.  if there is anything left in the socket buffer, it will stall for 1 minute per connection

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != 208 {
		return fmt.Errorf("server returned non-200 status: %v", resp.Status)
	}

	return nil
}

// For putting any Non-block files in storage
// NB- this never compresses- only block data is compressed
func putFile(client *http.Client, url string, textRaw string) error {

	req, err := http.NewRequest("PUT", url, bytes.NewReader([]byte(textRaw)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Authorization", g_config.AuthCode)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned non-200 status: %v", resp.Status)
	}

	return nil
}

// For putting any Non-block files in Cloud storage
// NB- this never compresses- only block data is compressed
func putFileCloud(fileName string, textRaw string) error {

	fullFile := fmt.Sprintf("%s/%s", g_config.Guid, fileName)

	sess := s3simple.Setup(g_config.AccessKey, g_config.SecretKey, g_config.ServerUrl, g_config.Region)
	log(DEBUG, fmt.Sprintf("About to put %s to bucket: %s", fileName, g_config.Bucket))
	s3simple.PutBlock(fullFile, []byte(textRaw), g_config.Bucket, &sess, true)

	return nil
}

func loadHCA(hcaFile string) []string {

	log(DEBUG, fmt.Sprintf("Loading HCA file:%s", hcaFile))

	var hcl []string
	if _, err := os.Stat(hcaFile); err == nil {
		hclFile, err := os.ReadFile(hcaFile)
		if err != nil {
			fatal(fmt.Sprintf("Error parsing HCA file:%s", err))
		}

		raw := string(hclFile)

		index := strings.Index(raw, "CODA")
		if index != -1 {
			raw = raw[:index]
		} else {
			fatal("HCA file is malformed (missing CODA)")
		}

		//var tmp []string
		tmp := strings.Split(raw, "\n")

		for _, str := range tmp {
			if str != "" {
				hcl = append(hcl, str)
			}
		}

		log(DEBUG, fmt.Sprintf("HCA loaded from file:%s", hcaFile))

	} else {
		log(INFO, "No "+hcaFile+" HCA found: baseline/initial")
	}
	return hcl
}

func loadCBT(cbtFile string) []string {
	var xcl []string
	if _, err := os.Stat(cbtFile); err == nil {
		xclFile, err := os.ReadFile(cbtFile)
		if err != nil {
			fatal(fmt.Sprintf("Error parsing xcl file:%s", err))
		}
		xcl = strings.Split(string(xclFile), "\n")
		xcl = xcl[:len(xcl)-2]
	} else {
		log(INFO, fmt.Sprintf("No xcl found (%s) CBT not available", cbtFile))
	}
	return xcl
}

func log(level LogLevel, msg string) error {

	if level > g_config.LogLevel {
		return nil
	}

	// Define the log file path
	logFilePath := filepath.Join(g_config.WorkDir, "/work.log")

	logMutex.Lock()
	defer logMutex.Unlock()
	// Open the log file in append mode
	file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	timestamp := time.Now().Format(time.RFC3339)
	entry := fmt.Sprintf("<%d> %s: %s",
		level,
		timestamp,
		msg)

	// Write the log entry to the file
	_, err = file.WriteString(entry + "\n")
	if err != nil {
		return err
	}

	return nil
}

func fatal(msg string) {
	fmt.Println(msg)
	log(ERROR, "Fatal error: "+msg)
	os.Exit(1)
}

func compress(textRaw []byte) (bytes.Buffer, error) {

	var b bytes.Buffer

	useZlib := false
	if useZlib == true {

		encoder := zlib.NewWriter(&b)
		encoder.Write([]byte(textRaw))
		encoder.Close()

	} else {

		encoder, _ := zstd.NewWriter(&b)
		encoder.Write([]byte(textRaw))
		encoder.Close()
	}

	return b, nil
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

func writeStats() {

	cur := time.Now()
	since := cur.Sub(g_lastStat)
	if since.Milliseconds() < 20 {
		return
	}
	elapsed := cur.Sub(g_start)

	var numer = int64(g_stats.NumBlocksProcessed)
	var denom = int64(g_stats.NumBlocks)
	var percent = decimal.NewFromInt(0)
	if denom > 0 && numer > 0 {
		percent = decimal.NewFromInt(numer).Div(decimal.NewFromInt(denom)).Mul(decimal.NewFromInt(100))
	}

	g_stats.Progress = percent.StringFixed(2)
	if g_stats.NumBlocks == g_stats.NumBlocksProcessed {
		g_stats.Progress = "100"
	} // avoid the occasional 99.9999

	secDec := decimal.NewFromFloat(elapsed.Seconds())
	g_stats.SecondsTotal = secDec.StringFixed(2)
	//	g_stats.SecondsTotal = float64(elapsed.Seconds())

	b, err := json.Marshal(g_stats)
	if err != nil {
		log(ERROR, fmt.Sprintf("Error marshalling stats.json: %s", err))
		return
	}
	//	fmt.Println("I just wrote the stats file ", g_stats)
	fn := fmt.Sprintf("%s/stats.json", g_config.WorkDir)
	if err := os.WriteFile(fn, b, 0666); err != nil {
		fmt.Println("Failed to write stats.json: ", err)
	}
	g_lastStat = cur
}

func isURL(url string) bool {
	if strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://") {
		return true
	}
	return false
}

func isLocalPath(str string) bool {
	return filepath.IsAbs(str)
}

func copyFile(src string, dest string) error {
	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer destination.Close()

	_, err = io.Copy(destination, source)
	if err != nil {
		return err
	}
	return nil
}

func sendAssets(client *http.Client) {
	stFile := fmt.Sprintf("%s/stats.json", g_config.WorkDir)
	swFile := fmt.Sprintf("%s/work.log", g_config.WorkDir)
	if g_config.ServerType == 0 {
		lPath := fmt.Sprintf("%s/%d/stats.json", g_config.ServerUrl, g_config.Timestamp)
		os.MkdirAll(filepath.Dir(lPath), 0755)
		copyFile(stFile, lPath)
		lPath = fmt.Sprintf("%s/%d/work.log", g_config.ServerUrl, g_config.Timestamp)
		copyFile(swFile, lPath)
	} else if g_config.ServerType == 1 {

		content, _ := os.ReadFile(stFile)
		barf := string(content)
		lPath := fmt.Sprintf("%s/file/%d/stats.json", g_config.ServerUrl, g_config.Timestamp)
		putFile(client, lPath, barf)

		content, _ = os.ReadFile(swFile)
		barf = string(content)
		lPath = fmt.Sprintf("%s/file/%d/work.log", g_config.ServerUrl, g_config.Timestamp)
		putFile(client, lPath, barf)
	} else if g_config.ServerType == 2 {
		content, _ := os.ReadFile(stFile)
		barf := string(content)

		lPath := fmt.Sprintf("%d/stats.json", g_config.Timestamp)
		putFileCloud(lPath, barf)

		content, _ = os.ReadFile(swFile)
		barf = string(content)

		lPath = fmt.Sprintf("%d/work.log", g_config.Timestamp)
		putFileCloud(lPath, barf)
	}
}

func getDiskSizeWin(driveIndex int) (int64, error) {

	cmd := exec.Command("powershell", "-Command",
		fmt.Sprintf("(Get-Disk -Number %d).Size", driveIndex+1))

	// Run the command and capture the output
	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	// Convert output to string and trim whitespace
	sizeStr := strings.TrimSpace(string(output))
	size, err := strconv.ParseInt(sizeStr, 10, 64)
	log(INFO, fmt.Sprintf("Converting disk size string from:%s to %d (%s)", sizeStr, size, output))
	if err != nil {
		return 0, err
	}

	return size, nil

}
