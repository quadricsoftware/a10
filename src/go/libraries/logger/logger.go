package logger

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

var (
	logFH     *os.File
	plogger   *log.Logger
	mu        sync.Mutex
	fileLock  *flock.Flock
	hostname  string
	component string
)

type SyslogPriority int

const (
	LOG_EMERG SyslogPriority = iota
	LOG_ALERT
	LOG_CRIT
	LOG_ERR
	LOG_WARNING
	LOG_NOTICE
	LOG_INFO
	LOG_DEBUG
)

const (
	LOG_USER SyslogPriority = 1 << 3
)

func Init(logFile string, who string) error {
	mu.Lock()
	defer mu.Unlock()

	if plogger != nil {
		return nil // Already initialized
	}

	// Ensure the directory exists
	dir := filepath.Dir(logFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %v", err)
	}

	// Create or open the log
	var err error
	logFH, err = os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %v", err)
	}

	fileLock = flock.New(logFile + ".go.lock")

	//plogger = log.New(logFH, "", 0) // We'll format the prefix ourselves
	log.SetOutput(logFH)

	component = who
	// Get hostname
	hostname, err = os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	return nil
}

// Log one line (syslog syntax)
func Log(priority SyslogPriority, message string) {

	mu.Lock()
	defer mu.Unlock()

	locked, err := fileLock.TryLock()
	if err != nil {
		log.Printf("Error acquiring file lock: %v\n", err)
		return
	}
	if !locked {
		log.Println("Could not acquire file lock, skipping log write")
		return
	}
	defer fileLock.Unlock()

	timestamp := time.Now().Format(time.RFC3339)
	pid := os.Getpid()

	// Format: <priority>version timestamp hostname app-name procid msgid structured-data msg
	// we don't need/use msgid or structured-data
	entry := fmt.Sprintf("<%d>1 %s %s %s %d - - %s",
		priority,
		timestamp,
		hostname,
		component,
		pid,
		message)
	log.Println(entry)
}

// Wrappers for logging comfort
func Debug(message string) {
	Log(LOG_USER|LOG_DEBUG, message)
}

func Info(message string) {
	Log(LOG_USER|LOG_INFO, message)
}

func Warn(message string) {
	Log(LOG_USER|LOG_WARNING, message)
}

func Error(message string) {
	Log(LOG_USER|LOG_ERR, message)
}

// Close closes the log file
func Close() {
	mu.Lock()
	defer mu.Unlock()

	if logFH != nil {
		logFH.Close()
	}
}
