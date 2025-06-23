package dag

import (
	"bytes"
	"context"
	"fmt"
	"github.com/robfig/cron/v3"
	"os"
	"sync"
	"time"
)

type Context struct {
	DAG *DAG
}

func (c *Context) SetXCom(key string, val interface{}) {
	c.DAG.xcomLock.Lock()
	defer c.DAG.xcomLock.Unlock()
	c.DAG.xcom[key] = val
}

func (c *Context) GetXCom(key string) interface{} {
	c.DAG.xcomLock.Lock()
	defer c.DAG.xcomLock.Unlock()
	return c.DAG.xcom[key]
}

type Job struct {
	ID         string
	Action     func(ctx *Context)
	BranchFunc func(ctx *Context) []string
	Depends    []*Job
	status     string
	statusLock sync.Mutex
}

func (j *Job) DependsOn(parents ...*Job) {
	j.Depends = append(j.Depends, parents...)
}

func (j *Job) Then(next *Job) *Job {
	next.DependsOn(j)
	return next
}

func (j *Job) Branch(nexts ...*Job) {
	for _, next := range nexts {
		next.DependsOn(j)
	}
}

type DAG struct {
	Name       string
	Schedule   string
	Jobs       []*Job
	xcom       map[string]interface{}
	xcomLock   sync.Mutex
	wg         sync.WaitGroup
	jobMap     map[string]*Job
	logFile    *os.File
	logLock    sync.Mutex
	activeJobs map[string]bool
}

var (
	dagRegistry  = make(map[string]*DAG)
	registryLock sync.RWMutex
)

func Register(d *DAG) {
	registryLock.Lock()
	defer registryLock.Unlock()
	dagRegistry[d.Name] = d
}

func (d *DAG) logLine(level, msg string) {
	line := fmt.Sprintf("[%s] [%s] %s\n", time.Now().Format("2006-01-02 15:04:05"), level, msg)
	d.logLock.Lock()
	defer d.logLock.Unlock()
	d.logFile.WriteString(line)
	d.logFile.Sync()
}

func (d *DAG) Logf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	d.logLine("INFO", msg)
}

func (d *DAG) LogErrorf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	d.logLine("ERROR", msg)
}

func Get(name string) (*DAG, bool) {
	registryLock.RLock()
	defer registryLock.RUnlock()
	d, ok := dagRegistry[name]
	return d, ok
}

func ListDAGs() []*DAG {
	registryLock.RLock()
	defer registryLock.RUnlock()
	var list []*DAG
	for _, d := range dagRegistry {
		list = append(list, d)
	}
	return list
}

func NewDAG(name, schedule string) *DAG {
	return &DAG{
		Name:       name,
		Schedule:   schedule,
		xcom:       make(map[string]interface{}),
		jobMap:     make(map[string]*Job),
		activeJobs: make(map[string]bool),
	}
}

func (d *DAG) NewJob(id string, action func(ctx *Context)) *Job {
	job := &Job{ID: id, Action: action, status: "pending"}
	d.Jobs = append(d.Jobs, job)
	d.jobMap[id] = job
	return job
}

func (d *DAG) NewBranchJob(id string, branchFunc func(ctx *Context) []string) *Job {
	job := &Job{ID: id, BranchFunc: branchFunc, status: "pending"}
	d.Jobs = append(d.Jobs, job)
	d.jobMap[id] = job
	return job
}
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
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		d.Logf("Cron triggered for DAG %s", d.Name)
		go d.runDAGWithContext(ctx)
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
	d.activeJobs = make(map[string]bool)
	d.Logf("Running DAG: %s", d.Name)
	myCtx := &Context{DAG: d}
	var jobWg sync.WaitGroup

	for _, job := range d.Jobs {
		job.setStatus("pending")
	}

	for _, job := range d.Jobs {
		if len(job.Depends) == 0 {
			jobWg.Add(1)
			go d.runJobWithContext(job, myCtx, &jobWg, ctx)
		}
	}
	jobWg.Wait()
}

func (d *DAG) runJobWithContext(j *Job, ctx *Context, jobWg *sync.WaitGroup, dagCtx context.Context) {
	defer jobWg.Done()

	if len(d.activeJobs) > 0 {
		parentsSucceeded := true
		for _, dep := range j.Depends {
			if dep.getStatus() != "success" {
				parentsSucceeded = false
				break
			}
		}
		if !parentsSucceeded {
			return
		}

		if !d.activeJobs[j.ID] {
			d.activeJobs[j.ID] = true
		}
	}

	for _, dep := range j.Depends {
		for dep.getStatus() != "success" {
			select {
			case <-dagCtx.Done():
				d.LogErrorf("Job %s canceled", j.ID)
				return
			default:
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	j.setStatus("running")
	d.Logf("Starting job %s", j.ID)

	defer func() {
		if r := recover(); r != nil {
			d.LogErrorf("Job %s panic: %v", j.ID, r)
			j.setStatus("failed")
		}
	}()

	if j.Action != nil {
		j.Action(ctx)
		j.setStatus("success")
		d.Logf("Completed job %s", j.ID)
	} else if j.BranchFunc != nil {
		selected := j.BranchFunc(ctx)
		d.Logf("Branch %s selected %v", j.ID, selected)

		for _, selID := range selected {
			d.markDownstreamActive(selID)
		}

		// Auto-skip child branch
		for _, job := range d.Jobs {
			if job == nil || job.BranchFunc != nil {
				continue
			}
			if isBranchChild(job) && !contains(selected, job.ID) {
				d.Logf("Skipping job %s (auto-success to unblock DAG)", job.ID)
				job.setStatus("success")
			}
		}

		j.setStatus("success")

		for _, selID := range selected {
			if child, ok := d.jobMap[selID]; ok {
				jobWg.Add(1)
				go d.runJobWithContext(child, ctx, jobWg, dagCtx)
			}
		}
		return
	} else {
		j.setStatus("success")
		d.Logf("Completed job %s", j.ID)
	}

	for _, child := range d.Jobs {
		for _, dep := range child.Depends {
			if dep == j {
				allDepOk := true
				for _, dep := range child.Depends {
					if dep.getStatus() != "success" {
						allDepOk = false
						break
					}
				}
				if allDepOk {
					jobWg.Add(1)
					go d.runJobWithContext(child, ctx, jobWg, dagCtx)
				}
			}
		}
	}
}

func isBranchChild(j *Job) bool {
	for _, dep := range j.Depends {
		if dep.BranchFunc != nil {
			return true
		}
	}
	return false
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func (d *DAG) markDownstreamActive(jobID string) {
	if d.activeJobs[jobID] {
		return
	}
	d.activeJobs[jobID] = true

	if job, ok := d.jobMap[jobID]; ok {
		for _, child := range d.getChildren(job) {
			d.markDownstreamActive(child.ID)
		}
	}
}


func (d *DAG) RerunDAG() {
	d.Logf("Rerunning DAG: %s", d.Name)
	for _, job := range d.Jobs {
		job.setStatus("pending")
	}
	ctx := &Context{DAG: d}
	var jobWg sync.WaitGroup
	for _, job := range d.Jobs {
		if len(job.Depends) == 0 {
			jobWg.Add(1)
			go d.runJobWithContext(job, ctx, &jobWg, context.Background())
		}
	}
	jobWg.Wait()
}

func (d *DAG) RerunJob(jobID string, downstream bool, upstream bool) {
	target, ok := d.jobMap[jobID]
	if !ok {
		d.LogErrorf("[ERROR] Job %s not found", jobID)
		return
	}
	d.Logf("Rerunning job %s (downstream=%v, upstream=%v)", jobID, downstream, upstream)

	// Reset status
	visited := make(map[string]bool)
	if upstream {
		d.resetUpstream(target, visited)
	}
	if downstream {
		d.resetDownstream(target, visited)
	}
	if !upstream && !downstream {
		target.setStatus("pending")
	}

	// Run
	ctx := &Context{DAG: d}
	var jobWg sync.WaitGroup
	jobWg.Add(1)
	go d.runJobWithContext(target, ctx, &jobWg, context.Background())
	jobWg.Wait()
}

func (d *DAG) resetUpstream(job *Job, visited map[string]bool) {
	if visited[job.ID] {
		return
	}
	visited[job.ID] = true
	job.setStatus("pending")
	for _, dep := range job.Depends {
		d.resetUpstream(dep, visited)
	}
}

func (d *DAG) resetDownstream(job *Job, visited map[string]bool) {
	if visited[job.ID] {
		return
	}
	visited[job.ID] = true
	job.setStatus("pending")
	for _, child := range d.getChildren(job) {
		d.resetDownstream(child, visited)
	}
}

func (j *Job) getStatus() string {
	j.statusLock.Lock()
	defer j.statusLock.Unlock()
	return j.status
}

func (j *Job) setStatus(s string) {
	j.statusLock.Lock()
	defer j.statusLock.Unlock()
	j.status = s
}

func (d *DAG) PrintGraph() {
	fmt.Printf("DAG: %s\n", d.Name)
	visited := make(map[string]bool)
	for _, job := range d.Jobs {
		if len(job.Depends) == 0 {
			d.printJobTree(job, "", true, visited)
		}
	}
}

func (d *DAG) printJobTree(j *Job, prefix string, isLast bool, visited map[string]bool) {
	connector := "├──"
	newPrefix := prefix + "│   "
	if isLast {
		connector = "└──"
		newPrefix = prefix + "    "
	}

	fmt.Printf("%s%s %s\n", prefix, connector, j.ID)

	children := d.getChildren(j)
	for i, child := range children {
		last := i == len(children)-1
		d.printJobTree(child, newPrefix, last, visited)
	}
}

func (d *DAG) getChildren(j *Job) []*Job {
	var children []*Job
	for _, job := range d.Jobs {
		for _, dep := range job.Depends {
			if dep == j {
				children = append(children, job)
			}
		}
	}
	return children
}
