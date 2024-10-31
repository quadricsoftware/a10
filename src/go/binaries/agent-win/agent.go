//go:build windows
// +build windows

package main

import (
	server "banana/libraries/agent-server"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type Config struct {
	Port          string `json:"port,omitempty"`
	APIKey        string `json:"apiKey,omitempty"`
	AllowedAccess string `json:"allowedAccess,omitempty"`
	LogFile       string `json:"logFile,omitempty"`
}

/*
Example json config file

	{
		"port":	"8888",
		"apiKey": "secret_api_key",
		"allowedAccess": "192.168.1.0/24",
		"logFile": "/my/location/logfile.log"
	}
*/
var config *Config

func loadConfig(file string) error {
	defaultConfig := &Config{
		Port:          "8888",
		APIKey:        "ALIKE",
		LogFile:       "agent.log",
		AllowedAccess: "",
	}

	data, err := os.ReadFile(file)
	if err != nil {
		log.Printf("Config file %s not found.  Loading defaults", file)
		config = defaultConfig
		return nil
	}

	config = &Config{}
	err = json.Unmarshal(data, config)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	//var port string
	//flag.StringVar(&port, "port", "8888", "Port to run the server on")
	flag.Parse()
	err := loadConfig("config.json")
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	port := config.Port
	srv := server.New(config.APIKey, config.AllowedAccess)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	file, err := os.OpenFile(config.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(file)

	go func() {
		log.Printf("Starting agent listener on port %s", port)
		if err := srv.Start(":" + string(port)); err != nil {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	if err := srv.Shutdown(); err != nil {
		log.Fatalf("Server shutdown failed: %v", err)
	}
	log.Println("Server stopped")
}
