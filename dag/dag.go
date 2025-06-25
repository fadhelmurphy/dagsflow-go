package dag

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
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

type TriggerRule string

const (
	AllSuccess TriggerRule = "all_success"
	AllFailed  TriggerRule = "all_failed"
	Always     TriggerRule = "always"
)

type JobStatus string

const (
	JobPending JobStatus = "PENDING"
	JobRunning JobStatus = "RUNNING"
	JobSuccess JobStatus = "SUCCESS"
	JobFailed  JobStatus = "FAILED"
	JobSkipped JobStatus = "SKIPPED"
)

type Job struct {
	ID          string
	action      func(ctx *Context)
	branchFunc  func(ctx *Context) []string
	depends     []*Job
	Upstreams   []*Job
	Downstreams []*Job
	TriggerRule TriggerRule
	status      JobStatus
	statusLock  sync.Mutex
}

func (j *Job) WithTriggerRule(rule TriggerRule) *Job {
	j.TriggerRule = rule
	return j
}

func (j *Job) dependsOn(parents ...*Job) {
	j.depends = append(j.depends, parents...)
}

func (j *Job) Then(next *Job) *Job {
	next.dependsOn(j)
	return next
}

func (j *Job) Branch(nexts ...*Job) {
	for _, next := range nexts {
		next.dependsOn(j)
	}
}

type Connection struct {
	Type   string
	Config map[string]any
}

type DAG struct {
	Name        string
	Schedule    string
	Config      map[string]any
	Jobs        []*Job
	xcom        map[string]any
	xcomLock    sync.Mutex
	wg          sync.WaitGroup
	jobMap      map[string]*Job
	logFile     *os.File
	logLock     sync.Mutex
	activeJobs  map[string]bool
	Connections map[string]*Connection
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

func (d *DAG) shouldRun(job *Job) bool {
	switch job.TriggerRule {
	case AllSuccess:
		for _, upstream := range job.Upstreams {
			if upstream.status != JobSuccess {
				return false
			}
		}
		return true
	case AllFailed:
		for _, upstream := range job.Upstreams {
			if upstream.status != JobFailed {
				return false
			}
		}
		return true
	case Always:
		return true
	default:
		// Default ke AllSuccess
		for _, upstream := range job.Upstreams {
			if upstream.status != JobSuccess {
				return false
			}
		}
		return true
	}
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

func NewDAG(name, schedule string, config ...map[string]any) *DAG {
	var cfg map[string]any
	if len(config) > 0 && config[0] != nil {
		cfg = config[0]
	} else {
		cfg = make(map[string]any)
	}
	return &DAG{
		Name:        name,
		Schedule:    schedule,
		Config:      cfg,
		xcom:        make(map[string]interface{}),
		jobMap:      make(map[string]*Job),
		activeJobs:  make(map[string]bool),
		Connections: make(map[string]*Connection),
	}
}

func LoadAllConnections(dir string) map[string]Connection {
	connections := make(map[string]Connection)

	entries, err := os.ReadDir(dir)
	if err != nil {
		fmt.Printf("Failed to read connections dir: %v\n", err)
		return connections
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			fmt.Printf("Failed to read connection file %s: %v\n", path, err)
			continue
		}

		var connMap map[string]Connection
		if err := json.Unmarshal(raw, &connMap); err != nil {
			fmt.Printf("Failed to parse connection file %s: %v\n", path, err)
			continue
		}

		// Merge ke connections global
		for k, v := range connMap {
			connections[k] = v
		}

		fmt.Printf("Loaded connections from %s\n", path)
	}

	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
		connections["env_bigquery"] = Connection{
			Type: "bigquery",
			Config: map[string]any{
				"credentials_path": os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"),
				"project_id":       os.Getenv("GOOGLE_CLOUD_PROJECT"),
			},
		}
		fmt.Println("Loaded env_bigquery connection from ENV")
	}

	return connections
}

func (d *DAG) TriggerDAGWithConfig(dagName string, config map[string]any, blocking bool) {
	if target, ok := Get(dagName); ok {
		d.Logf("Triggering DAG %s from DAG %s with config %v", dagName, d.Name, config)

		target.Config = config

		ctx := context.Background()
		if blocking {
			target.RunWithContext(ctx)
		} else {
			go target.RunWithContext(ctx)
		}
	} else {
		d.LogErrorf("DAG %s not found to trigger", dagName)
	}
}

func (d *DAG) NewJob(id string, action func(ctx *Context)) *Job {
	job := &Job{
		ID: id, 
		action: action, 
		status: "PENDING",
		TriggerRule:  Always,
	}
	d.Jobs = append(d.Jobs, job)
	d.jobMap[id] = job
	return job
}

func (d *DAG) NewBranchJob(id string, branchFunc func(ctx *Context) []string) *Job {
	job := &Job{ID: id, branchFunc: branchFunc, status: "PENDING"}
	d.Jobs = append(d.Jobs, job)
	d.jobMap[id] = job
	return job
}

func (d *DAG) TriggerDAG(dagName string) {
	if target, ok := Get(dagName); ok {
		d.Logf("Triggering DAG %s from DAG %s", dagName, d.Name)
		ctx := context.Background()
		go target.RunWithContext(ctx)
	} else {
		d.LogErrorf("DAG %s not found to trigger", dagName)
	}
}

func (d *DAG) TriggerDAGBlocking(dagName string) {
	if target, ok := Get(dagName); ok {
		d.Logf("Triggering DAG %s (blocking) from DAG %s", dagName, d.Name)
		ctx := context.Background()
		target.RunWithContext(ctx)
	} else {
		d.LogErrorf("DAG %s not found to trigger", dagName)
	}
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
		job.setStatus("PENDING")
	}

	for _, job := range d.Jobs {
		if len(job.depends) == 0 {
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
		for _, dep := range j.depends {
			if dep.getStatus() != "SUCCESS" {
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

	for _, dep := range j.depends {
		for dep.getStatus() != "SUCCESS" {
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
			j.setStatus("FAILED")
		}
	}()

	if j.action != nil {
		j.action(ctx)
		j.setStatus("SUCCESS")
		d.Logf("Completed job %s", j.ID)
	} else if j.branchFunc != nil {
		selected := j.branchFunc(ctx)
		d.Logf("Branch %s selected %v", j.ID, selected)

		for _, selID := range selected {
			d.markDownstreamActive(selID)
		}

		// Auto-skip child branch
		for _, job := range d.Jobs {
			if job == nil || job.branchFunc != nil {
				continue
			}
			if isBranchChild(job) && !contains(selected, job.ID) {
				d.Logf("Skipping job %s (auto-success to unblock DAG)", job.ID)
				job.setStatus("SKIPPED")
			}
		}

		j.setStatus("SUCCESS")

		for _, selID := range selected {
			if child, ok := d.jobMap[selID]; ok {
				jobWg.Add(1)
				go d.runJobWithContext(child, ctx, jobWg, dagCtx)
			}
		}
		return
	} else {
		j.setStatus("SUCCESS")
		d.Logf("Completed job %s", j.ID)
	}

	for _, child := range d.Jobs {
		for _, dep := range child.depends {
			if dep == j {
				allDepOk := true
				for _, dep := range child.depends {
					if dep.getStatus() != "SUCCESS" {
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
	for _, dep := range j.depends {
		if dep.branchFunc != nil {
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
		job.setStatus("PENDING")
	}
	ctx := &Context{DAG: d}
	var jobWg sync.WaitGroup
	for _, job := range d.Jobs {
		if len(job.depends) == 0 {
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
		target.setStatus("PENDING")
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
	job.setStatus("PENDING")
	for _, dep := range job.depends {
		d.resetUpstream(dep, visited)
	}
}

func (d *DAG) resetDownstream(job *Job, visited map[string]bool) {
	if visited[job.ID] {
		return
	}
	visited[job.ID] = true
	job.setStatus("PENDING")
	for _, child := range d.getChildren(job) {
		d.resetDownstream(child, visited)
	}
}

func (j *Job) getStatus() JobStatus {
	j.statusLock.Lock()
	defer j.statusLock.Unlock()
	return j.status
}

func (j *Job) setStatus(s JobStatus) {
	j.statusLock.Lock()
	defer j.statusLock.Unlock()
	j.status = s
}

func (d *DAG) PrintGraph() {
	fmt.Printf("DAG: %s\n", d.Name)
	visited := make(map[string]bool)
	for _, job := range d.Jobs {
		if len(job.depends) == 0 {
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
		for _, dep := range job.depends {
			if dep == j {
				children = append(children, job)
			}
		}
	}
	return children
}
