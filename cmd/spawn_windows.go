//go:build windows

package cmd

import (
	"os"
	"os/exec"
	"syscall"
)

func spawnDetached(dagName string) error {
	cmd := exec.Command(os.Args[0], "internal-run", dagName)
	cmd.Stdout = nil
	cmd.Stderr = nil


	cmd.SysProcAttr = &syscall.SysProcAttr{
		HideWindow:    true,
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP,
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	return writePidFiles(dagName, cmd.Process.Pid)
}
