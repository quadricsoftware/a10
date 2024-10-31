package handlers

import (
	"banana/libraries/sysLib"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	ps "github.com/mitchellh/go-ps"

	"github.com/gorilla/mux"
)

const (
	uploadDir     = "/tmp"
	maxUploadSize = 500 << 20 // 50 MB
)

var (
	runningProcesses = make(map[int]*exec.Cmd)
	processMutex     sync.Mutex
	secretApiKey     = ""
	allowedAccess    = ""
)

func checkApiKey(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.Header.Get("X-API-KEY")
		if apiKey != secretApiKey {
			http.Error(w, "Invalid API key", http.StatusUnauthorized)
			return
		}
		if allowedAccess != "" {
			clientIP, _, _ := net.SplitHostPort(r.RemoteAddr)
			if !allowedIP(clientIP, allowedAccess) {
				http.Error(w, "Access denied", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(w, r)
	}
}
func allowedIP(clientIP, allowedAccess string) bool {
	// Check if allowedAccess is an IP address
	if net.ParseIP(allowedAccess) != nil {
		return clientIP == allowedAccess
	}

	// Check if allowedAccess is a CIDR range
	_, ipnet, err := net.ParseCIDR(allowedAccess)
	if err == nil {
		clientIPAddr := net.ParseIP(clientIP)
		return ipnet.Contains(clientIPAddr)
	}

	// If neither IP nor CIDR, allow access
	return true
}

func SetupRoutes(r *mux.Router, apiKey string, acl string) {
	secretApiKey = apiKey
	allowedAccess = acl
	r.HandleFunc("/execute", checkApiKey(executeCommand))
	r.HandleFunc("/upload", checkApiKey(uploadFile))
	r.HandleFunc("/pid/{pid}", checkApiKey(pidCheck))
	r.HandleFunc("/download", checkApiKey(downloadFile))
	r.HandleFunc("/run/{filename}", checkApiKey(runProgram))
	//r.HandleFunc("/status/{pid}", checkApiKey(getProcessStatus))
	//r.HandleFunc("/kill/{pid}", checkApiKey(killProcess))
	r.HandleFunc("/{any:.*}", unknownHandler)
}

func unknownHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Unknown request", http.StatusNotFound)
}

type CommandRequest struct {
	Script string   `json:"script"`
	Args   []string `json:"args,omitempty"`
}
type CommandResponse struct {
	Output string `json:"output"`
	Error  string `json:"error,omitempty"`
}

func executeCommand(w http.ResponseWriter, r *http.Request) {
	var req CommandRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	log.Printf("req: %v\n", req)

	output, err := runCommand(req.Script)
	if err != nil {
		log.Printf("Error running command: %v.  Error: %v\n", req, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Output: %s\n", output)

	response := CommandResponse{
		Output: string(output),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// This is for scripted commands (eg. powershell)
func runCommand(command string) ([]byte, error) {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("powershell", "-Command", command)
	} else {
		cmd = exec.Command("/bin/bash", "-c", command)
	}
	return cmd.CombinedOutput()
}

// By default runProgram will block until the program exits.
// If background=true is sent, it will background it and return the pid
type RunProgramRequest struct {
	Args       []string `json:"args,omitempty"`
	Background bool     `json:"background,omitempty"`
}

type RunProgramResponse struct {
	Output string `json:"output,omitempty"`
	PID    int    `json:"pid,omitempty"`
}

// this is for any background procs so we can track and reap them to avoid zombies
var (
	backgroundProcesses sync.Map
)

func pidCheck(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	strpid := vars["pid"]
	pid, _ := strconv.Atoi(strpid)

	var running bool = false
	process, err := ps.FindProcess(pid)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	if process != nil {
		running = true
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"running": running})

}

// This can run any program on the system
// By default it will look in the $PATH first, and then in the CWD
// It can take no arguments, or many
func runProgram(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filename := vars["filename"]

	var req RunProgramRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// If there's an error decoding, assume no arguments and foreground execution
		req = RunProgramRequest{}
	}

	programPath, err := exec.LookPath(filename)
	if err != nil {
		// Program not found in PATH, try running from CWD
		log.Printf("Cant find program in path, looking locally: %s\n", filename)
		programPath = "./" + filename
	}

	log.Printf("\nRequest dump: %v\n", req)

	var cmd *exec.Cmd
	if len(req.Args) > 0 {
		log.Printf("Running with args: %s %v\n", filename, req.Args)
		cmd = exec.Command(programPath, req.Args...)
	} else {
		log.Printf("Running with NO  args: %s \n", filename)
		cmd = exec.Command(programPath)
	}

	if req.Background {

		cmd, err := sysLib.StartBackgroundProcess(programPath, req.Args...)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error starting program: %v", err), http.StatusInternalServerError)
			return
		}

		pid := cmd.Process.Pid
		backgroundProcesses.Store(pid, cmd)

		go func() {
			cmd.Wait()
			backgroundProcesses.Delete(pid) // reap lest we amass zombi
		}()

		response := RunProgramResponse{
			PID: pid,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	} else {
		// Run in foreground
		output, err := cmd.CombinedOutput()
		if err != nil {
			http.Error(w, fmt.Sprintf("Error executing program: %s, %s, %v\noutput: %s", programPath, req.Args, err, output), http.StatusInternalServerError)
			return
		}

		response := RunProgramResponse{
			Output: string(output),
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}

// Allow uploads to our CWD.
// Don't let them overwrite this running binary
// This allows for remote updates for other assets
func uploadFile(w http.ResponseWriter, r *http.Request) {
	file, header, err := r.FormFile("file")
	if err != nil {
		log.Printf("Error with upload request %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	selfPath, err := os.Executable()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	dstPath := header.Filename
	remotePath := r.FormValue("remotePath")
	if remotePath == "" {
		log.Println("No remotePath form value specified!")
		http.Error(w, "remotePath is required", http.StatusBadRequest)
		return
	} else {
		dstPath = remotePath
		dstDir := filepath.Dir(remotePath)
		if err := os.MkdirAll(dstDir, os.ModePerm); err != nil {
			log.Printf("Error creating directory: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	log.Printf("Uploading to: %s\n", dstPath)
	log.Printf("Header: %v\n", header.Filename)

	// Check if the destination path is the same as the current running binary
	if filepath.Clean(dstPath) == filepath.Clean(selfPath) {
		http.Error(w, "Cannot overwrite the current running binary!", http.StatusBadRequest)
		return
	}

	// Check if the destination path is "below" the CWD
	relPath, err := filepath.Rel(".", dstPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if strings.HasPrefix(relPath, ".."+string(os.PathSeparator)) {
		http.Error(w, "Cannot upload files outside of the current directory", http.StatusBadRequest)
		return
	}

	dst, err := os.Create(dstPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	if _, err := io.Copy(dst, file); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("File %s uploaded successfully\n", dstPath)
}

type DownloadRequest struct {
	Filename string `json:"filename"`
}

func downloadFile(w http.ResponseWriter, r *http.Request) {

	var req DownloadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	log.Printf("req: %v\n", req)

	filename := req.Filename

	if _, err := os.Stat(filename); os.IsNotExist(err) {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
	w.Header().Set("Content-Type", "application/octet-stream")

	http.ServeFile(w, r, filename)
}
