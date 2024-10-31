// process_linux.go
//go:build linux

package sysLib

import (
	"os/exec"
	"syscall"
)

func StartBackgroundProcess(command string, args ...string) (*exec.Cmd, error) {
	cmd := exec.Command(command, args...)

	// Set the process group ID to ensure it runs in the background
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	// Start the command
	err := cmd.Start()
	if err != nil {
		return nil, err
	}

	// Return the process ID
	return cmd, nil
}
