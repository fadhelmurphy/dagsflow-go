//go:build !windows

package cmd

import (
	"os"
	"os/exec"
	"syscall"
)

func spawnDetached(dagName string) error {
	cmd := exec.Command(os.Args[0], "internal-run", dagName)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	return writePidFiles(dagName, cmd.Process.Pid)
}
