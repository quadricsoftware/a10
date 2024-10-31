package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	//	"github.com/quadricsoftware/banana/s3simple"
	"banana/libraries/logger"
	"banana/libraries/s3simple"
	"errors"
	"os"
	"sync"

	//	"syscall"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Config struct {
	Endpoint   string
	Bucket     string
	AccessKey  string
	SecretKey  string
	Region     string
	Guid       string
	NumWorkers int
}

var logMutex = sync.Mutex{}

// var statMutex = sync.Mutex{}
var g_config Config

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s <source_file> <url/local QDS path> [block size MB] [num http clients]\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(2)
}

func loadConfig(configPath string) {

	//	fmt.Printf("Loading config file: %s\n", configPath)

	fileContent, err := os.ReadFile(configPath)
	if err != nil {
		fatal(fmt.Sprintf("Error reading file: %s", err))
		return
	}
	//fmt.Printf("Raw JSon: %s", fileContent)

	err = json.Unmarshal(fileContent, &g_config)
	if err != nil {
		fatal(fmt.Sprintf("Error unmarshalling JSON: %s", err))
		return
	}

	//	fmt.Printf("Unmarshalled: %+v", g_config)

	logger.Debug(fmt.Sprintf("Found URL: %s", g_config.Endpoint))

}

func main() {

	logger.Init("../logs/app.log", "cloudtool")
	g_config.NumWorkers = 4

	//sourceFile := ""
	targetPath := ""
	prefix := ""
	//guid := ""

	doList := ""
	doPut := ""
	doGet := ""
	doStat := ""
	doDel := ""

	confFile := flag.String("c", "", "Config File")

	flag.IntVar(&g_config.NumWorkers, "w", 4, "The number of coroutines")

	//flag.StringVar(&sourceFile, "s", "", "Data source path ")
	flag.StringVar(&targetPath, "o", "", "Data destination path")
	flag.StringVar(&prefix, "p", "", "Path to list (eg. 1719315112) Guid is always prepended")
	//flag.StringVar(&guid, "g", "", "Guid for cloud source (required for list)")

	flag.StringVar(&doList, "l", "", "List files for Guid ")
	flag.StringVar(&doPut, "put", "", "File to put to cloud (requires -o dest path ) ")
	flag.StringVar(&doGet, "get", "", "File to get from cloud (requires -o dest path ) ")
	flag.StringVar(&doStat, "stat", "", "Check if file exists in cloud ")
	flag.StringVar(&doDel, "del", "", "Delete file from cloud ")

	flag.Usage = usage
	flag.Parse()

	if *confFile != "" {
		loadConfig(*confFile)
	} else {
		fatal("No config file provided!")
		// this is for manual arg parsing
	}

	sess := s3simple.Setup(g_config.AccessKey, g_config.SecretKey, g_config.Endpoint, g_config.Region)

	res := make(map[string]any)
	res["result"] = "error"
	res["message"] = "Unknown error"

	if doStat != "" {
		res = statFile(doStat, &sess)
	} else if doPut != "" {
		res = putFile(doPut, targetPath, &sess)
	} else if doGet != "" {
		res = getFile(doGet, targetPath, &sess)
	} else if doDel != "" {
		res = deleteFile(doDel, &sess)
	} else if doList != "" {
		if doList == "/" {
			res = getDSList(&sess)
		} else {
			startPoint := fmt.Sprintf("%s/%s", doList, prefix)
			if prefix == "" {
				startPoint = fmt.Sprintf("%s", doList)
			}
			res = getFileList(startPoint, doList, &sess)
		}
	} else {
		res["message"] = "No command given!"
	}

	printJson(res)

	os.Exit(0)
}

func printJson(data map[string]any) {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
		return
	}
	fmt.Println(string(jsonData))
}

func getDSList(sess *s3.S3) map[string]any {

	ret := make(map[string]any)
	ret["result"] = "success"

	stuff, err := s3simple.ListEverything(g_config.Bucket, sess)
	if err != nil {
		msg := fmt.Sprintf("Error DS listing bucket. Err: %v", err)
		ret["result"] = "error"
		ret["message"] = msg
		logger.Warn(msg)
		return ret
	}

	uniqueFolders := make(map[string]bool)
	for _, thing := range stuff {
		//items = append(items, thing)
		parts := strings.Split(thing, "/")
		if len(parts) > 0 {
			folder := parts[0]
			if len(folder) == 36 {
				uniqueFolders[folder] = true
			}
		}
	}
	items := make([]string, 0, len(uniqueFolders))
	for folder := range uniqueFolders {
		items = append(items, folder)
	}

	ret["files"] = items
	return ret
}

func getFileList(startPoint string, guid string, sess *s3.S3) map[string]any {

	//fmt.Printf("Starting Point: %s\n", startPoint)

	ret := make(map[string]any)
	ret["result"] = "success"

	var items []string
	stuff, err := s3simple.ListBucket(startPoint, guid+"/blocks", g_config.Bucket, sess)
	if err != nil {
		msg := fmt.Sprintf("Error file listing bucket. Err: %v", err)
		ret["result"] = "error"
		ret["message"] = msg
		logger.Warn(msg)
		return ret
	}
	for _, thing := range stuff {
		items = append(items, thing)
	}

	ret["files"] = items
	return ret
}

func deleteFile(targetPath string, sess *s3.S3) map[string]any {
	ret := make(map[string]any)
	ret["result"] = "success"

	if targetPath == "" {
		ret["result"] = "error"
		ret["message"] = fmt.Sprint("No dest path given!")
		return ret
	}
	err := s3simple.DeleteFile(targetPath, g_config.Bucket, sess)
	if err != nil {
		msg := fmt.Sprintf("Error deleting file %s Err: %v", targetPath, err)
		ret["result"] = "error"
		ret["message"] = msg
		logger.Warn(msg)
	} else {
		msg := fmt.Sprintf("Deleted file %s ", targetPath)
		ret["message"] = msg
		logger.Debug(msg)
	}
	return ret
}

func getFile(remoteFile string, localFile string, sess *s3.S3) map[string]any {
	ret := make(map[string]any)
	ret["result"] = "success"

	if localFile == "" {
		ret["result"] = "error"
		ret["message"] = fmt.Sprint("No dest path given!")
		return ret
	}
	if remoteFile == "" {
		ret["result"] = "error"
		ret["message"] = fmt.Sprint("No source file given!")
		return ret
	}
	b, err := s3simple.GetFile(remoteFile, g_config.Bucket, sess)
	if err != nil {
		msg := fmt.Sprintf("Error getting file: %s  ", err)
		ret["result"] = "error"
		ret["message"] = msg
		logger.Debug(msg)
		return ret
	}
	err = os.WriteFile(localFile, b, 0644)
	if err != nil {
		msg := fmt.Sprintf("Failed to write file locally: %s  ", err)
		ret["result"] = "error"
		ret["message"] = msg
		logger.Warn(msg)
		return ret
	}
	ret["result"] = "success"
	ret["message"] = fmt.Sprintf("Got %s to %s", remoteFile, localFile)
	return ret
}

func putFile(remoteFile string, localFile string, sess *s3.S3) map[string]any {
	ret := make(map[string]any)
	ret["result"] = "success"

	if remoteFile == "" {
		ret["result"] = "error"
		ret["message"] = fmt.Sprint("No dest path given!")
		return ret
	}
	if localFile == "" {
		ret["result"] = "error"
		ret["message"] = fmt.Sprint("No source file given!")
		return ret
	}
	_, error := os.Stat(localFile)
	if errors.Is(error, os.ErrNotExist) {
		ret["result"] = "error"
		ret["message"] = fmt.Sprintf("Source file not found: %s", localFile)
		return ret
	}
	b, err := os.ReadFile(localFile)
	if err != nil {
		ret["result"] = "error"
		ret["message"] = fmt.Sprintf("Couldn't load source: %v", err)
		return ret
	}
	err = s3simple.PutBlock(remoteFile, b, g_config.Bucket, sess, true)
	if err != nil {
		msg := fmt.Sprintf("Error connecting to bucket: %v", err)
		ret["result"] = "error"
		ret["message"] = msg
		logger.Error(msg)
	} else {
		msg := fmt.Sprintf("Put %s to %s", localFile, remoteFile)
		ret["result"] = "success"
		ret["message"] = msg
		logger.Debug(msg)
	}
	return ret

}
func statFile(fileName string, sess *s3.S3) map[string]any {
	ret := make(map[string]any)

	ret["result"] = "success"
	if fileName == "" {
		ret["result"] = "error"
		ret["message"] = "No file given"
		return ret
	}
	//fmt.Printf("Stating file %s\n\n", targetPath)
	haveIt, err := s3simple.HaveFile(fileName, g_config.Bucket, sess)
	if err != nil {
		msg := fmt.Sprintf("Error connecting to bucket: %v", err)
		ret["result"] = "error"
		ret["message"] = msg
		logger.Debug(msg)

	} else {
		if haveIt {
			ret["result"] = "success"
			ret["message"] = "true"
		} else {
			ret["result"] = "success"
			ret["message"] = "false"
		}
	}
	return ret
}

func fatal(msg string) {
	fmt.Println(msg)
	logger.Error("Fatal error: " + msg)
	os.Exit(1)
}
