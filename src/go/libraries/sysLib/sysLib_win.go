// process_windows.go
//go:build windows

package sysLib

import (
	"os/exec"
	"syscall"
)

func StartBackgroundProcess(command string, args ...string) (*exec.Cmd, error) {
	cmd := exec.Command(command, args...)

	// Set the process to create a new process group and run in the background
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | 0x08000000, // 0x08000000 is DETACHED_PROCESS
	}

	// Start the command
	err := cmd.Start()
	if err != nil {
		return nil, err
	}

	return cmd, nil
}
