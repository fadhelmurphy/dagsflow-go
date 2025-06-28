package dag

import (
	"github.com/robfig/cron/v3"
	"fmt"
	"os"
	"context"
	"sync"
	"bytes"
	"time"
)

func (d *DAG) Run() {
	os.MkdirAll("logs", 0755)
	logPath := fmt.Sprintf("logs/%s.log", d.Name)
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Failed to open log file: %v\n", err)
		return
	}
	d.logFile = f
	defer d.logFile.Close()

	// Redirect stdout
	r, w, _ := os.Pipe()
	oldStdout := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

	go func() {
		buf := make([]byte, 1024)
		for {
			n, err := r.Read(buf)
			if err != nil {
				return
			}
			line := string(bytes.TrimRight(buf[:n], "\n"))
			d.logLine("INFO", line)
		}
	}()

	c := cron.New()
	c.AddFunc(d.Schedule, func() {
		if !d.trySetRunning() {
			d.Logf("DAG %s is still running, skipping new trigger", d.Name)
			return
		}

		d.Logf("Cron triggered for DAG %s", d.Name)
		d.activeJobsLock.Lock()
		d.activeJobs = make(map[string]bool)
		d.activeJobsLock.Unlock()

		go func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			d.runDAGWithContext(ctx)
			d.unsetRunning()
		}()
	})

	c.Start()

	// Keep the process alive
	select {}
}

func (d *DAG) RunWithContext(ctx context.Context) {
	os.MkdirAll("logs", 0755)
	logPath := fmt.Sprintf("logs/%s.log", d.Name)
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Failed to open log file: %v\n", err)
		return
	}
	d.logFile = f
	defer d.logFile.Close()

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.runDAGWithContext(ctx)
	}()
	d.wg.Wait()
}

func (d *DAG) runDAGWithContext(ctx context.Context) {
	d.activeJobsLock.Lock()
	d.activeJobs = make(map[string]bool)
	d.activeJobsLock.Unlock()
	d.Logf("Running DAG: %s", d.Name)

	myCtx := &Context{DAG: d}
	var jobWg sync.WaitGroup

	for _, job := range d.Jobs {
		job.setStatus(JobPending)
	}

	for _, job := range d.Jobs {
		if len(job.depends) == 0 && d.shouldRun(job) {
			jobWg.Add(1)
			go d.runJobWithContext(job, myCtx, &jobWg, ctx)
		}
	}

	jobWg.Wait()
	d.Logf("DAG %s finished.", d.Name)
	d.unsetRunning()
}

func (d *DAG) waitUntilParentsDone(j *Job, dagCtx context.Context) bool {
	for {
		allDone := true
		for _, dep := range j.depends {
			status := dep.getStatus()
			if status != JobSuccess && status != JobFailed && status != JobSkipped {
				allDone = false
				break
			}
		}
		if allDone {
			return true
		}
		select {
		case <-dagCtx.Done():
			d.LogErrorf("Job %s canceled before parent finished", j.ID)
			return false
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}
}

func (d *DAG) runJobWithContext(j *Job, ctx *Context, jobWg *sync.WaitGroup, dagCtx context.Context) {
	defer jobWg.Done()

	if !d.waitUntilParentsDone(j, dagCtx) {
		return
	}

	if !d.shouldRun(j) {
		j.setStatus(JobSkipped)
		d.Logf("Skipping job %s due to unmet trigger rule", j.ID)
		return
	}

	d.markJobActive(j)

	j.setStatus(JobRunning)
	d.Logf("Starting job %s", j.ID)

	defer d.recoverFromPanic(j)

	if j.action != nil {
		d.runActionJob(j, ctx)
	} else if j.branchFunc != nil {
		d.runBranchJob(j, ctx, jobWg, dagCtx)
		return
	} else {
		j.setStatus(JobSuccess)
		d.Logf("Completed job %s", j.ID)
	}

	d.triggerChildrenIfReady(j, ctx, jobWg, dagCtx)
}

func (d *DAG) triggerChildrenIfReady(j *Job, ctx *Context, jobWg *sync.WaitGroup, dagCtx context.Context) {
	for _, child := range d.getChildren(j) {
		if d.shouldRun(child) {
			jobWg.Add(1)
			go d.runJobWithContext(child, ctx, jobWg, dagCtx)
		} else {
			child.setStatus(JobSkipped)
			d.Logf("Skipping job %s due to unmet trigger rule", child.ID)
		}
	}
}
