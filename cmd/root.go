package cmd

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"dagsflow-go/dag"

	"github.com/spf13/cobra"
)

func writePidFiles(dagName string, pid int) error {
	os.MkdirAll("dagsflow-pid", 0755)
	pidFile := fmt.Sprintf("dagsflow-pid/%s.pid", dagName)
	runningFile := fmt.Sprintf("dagsflow-pid/%s.running", dagName)

	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(pid)), 0644); err != nil {
		return err
	}
	if err := os.WriteFile(runningFile, []byte("running"), 0644); err != nil {
		return err
	}
	return nil
}


func stopByPidFile(dagName string) error {
	pidFile := fmt.Sprintf("dagsflow-pid/%s.pid", dagName)
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return fmt.Errorf("pid file not found")
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(data)))
	proc, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("failed to find process: %v", err)
	}

	// Langsung kill (Windows style)
	if err := proc.Kill(); err != nil {
		return fmt.Errorf("failed to kill process: %v", err)
	}

	os.Remove(pidFile)
	os.Remove(fmt.Sprintf("dagsflow-pid/%s.running", dagName))
	return nil
}


var rootCmd = &cobra.Command{
	Use:   "dagsflow-go",
	Short: "DAG scheduler like Airflow in Go",
}

// run [dag-name]
var runCmd = &cobra.Command{
	Use:   "run [dag-name]",
	Short: "Run DAG detached",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dagName := args[0]
		fmt.Printf("Starting DAG %s in background...\n", dagName)
		err := spawnDetached(dagName)
		if err != nil {
			fmt.Println("Failed to start:", err)
		}
	},
}

// internal-run
var internalRunCmd = &cobra.Command{
	Use:   "internal-run [dag-name]",
	Short: "Internal runner (do not call manually)",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dagName := args[0]
		d, ok := dag.Get(dagName)
		if !ok {
			fmt.Println("DAG not found:", dagName)
			return
		}

		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-c
			fmt.Println("Stopping DAG:", dagName)
			os.Exit(0)
		}()

		d.Run()
		select {}
	},
}

// stop
var stopCmd = &cobra.Command{
	Use:   "stop [dag-name]",
	Short: "Stop a running DAG",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dagName := args[0]
		err := stopByPidFile(dagName)
		if err != nil {
			fmt.Println("Failed to stop:", err)
		} else {
			fmt.Println("Stopped DAG:", dagName)
		}
	},
}

// list
var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List DAGs and status",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("%-15s | %-10s | %-20s\n", "DAG Name", "Is Running", "Schedule")
		fmt.Println(strings.Repeat("-", 15) + "-|-" + strings.Repeat("-", 10) + "-|-" + strings.Repeat("-", 20))
		for _, d := range dag.ListDAGs() {
			running := checkRunning(d.Name)
			fmt.Printf("%-15s | %-10t | %-20s\n", d.Name, running, d.Schedule)
		}
	},
}

func checkRunning(dagName string) bool {
	runningFile := fmt.Sprintf("dagsflow-pid/%s.running", dagName)
	_, err := os.Stat(runningFile)
	return err == nil
}

// run-all
var runAllCmd = &cobra.Command{
	Use:   "run-all",
	Short: "Run all DAGs detached",
	Run: func(cmd *cobra.Command, args []string) {
		for _, d := range dag.ListDAGs() {
			fmt.Printf("Starting DAG %s in background...\n", d.Name)
			err := spawnDetached(d.Name)
			if err != nil {
				fmt.Printf("Failed to start DAG %s: %v\n", d.Name, err)
			}
		}
	},
}

// stop-all
var stopAllCmd = &cobra.Command{
	Use:   "stop-all",
	Short: "Stop all running DAGs",
	Run: func(cmd *cobra.Command, args []string) {
		for _, d := range dag.ListDAGs() {
			err := stopByPidFile(d.Name)
			if err != nil {
				fmt.Printf("Failed to stop DAG %s: %v\n", d.Name, err)
			} else {
				fmt.Printf("Stopped DAG %s\n", d.Name)
			}
		}
	},
}

// graph [dag-name]
var graphCmd = &cobra.Command{
	Use:   "graph [dag-name]",
	Short: "Print DAG graph",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		d, ok := dag.Get(args[0])
		if !ok {
			fmt.Printf("DAG %s not found\n", args[0])
			return
		}
		d.PrintGraph()
	},
}
var logCmd = &cobra.Command{
	Use:   "log [dag-name]",
	Short: "Show realtime log output of a DAG",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dagName := args[0]
		logFile := fmt.Sprintf("logs/%s.log", dagName)

		f, err := os.Open(logFile)
		if err != nil {
			fmt.Printf("Failed to open log file: %v\n", err)
			return
		}
		defer f.Close()

		// Read History
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
		}

		// Tail realtime
		reader := bufio.NewReader(f)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			fmt.Print(line)
		}
	},
}

var rerunDagCmd = &cobra.Command{
	Use: "rerun-dag [dag_name]",
	Short: "Rerun entire DAG",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			fmt.Println("Please provide DAG name")
			return
		}
		d, ok := dag.Get(args[0])
		if !ok {
			d.LogErrorf("DAG not found")
			return
		}
		d.RerunDAG()
	},
}

var rerunJobCmd = &cobra.Command{
	Use: "rerun-job [dag_name] [job_id]",
	Short: "Rerun job in DAG",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			fmt.Println("Please provide DAG name and job ID")
			return
		}
		d, ok := dag.Get(args[0])
		if !ok {
			fmt.Println("DAG not found")
			return
		}
		jobID := args[1]
		downstream, _ := cmd.Flags().GetBool("downstream")
		upstream, _ := cmd.Flags().GetBool("upstream")
		d.RerunJob(jobID, downstream, upstream)
	},
}


func Execute() {
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(runAllCmd)
	rootCmd.AddCommand(stopCmd)
	rootCmd.AddCommand(stopAllCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(graphCmd)
	rootCmd.AddCommand(internalRunCmd)
	rootCmd.AddCommand(logCmd)
	rerunJobCmd.Flags().Bool("downstream", false, "Rerun downstream jobs")
	rerunJobCmd.Flags().Bool("upstream", false, "Rerun upstream jobs")

	rootCmd.AddCommand(rerunDagCmd)
	rootCmd.AddCommand(rerunJobCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
