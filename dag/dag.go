package dag

import (
	"bytes"
	"context"
	"dagsflow-go/operator"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"text/template"

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

func (j *Job) WithTriggerRule(rule TriggerRule) *Job {
	j.TriggerRule = rule
	return j
}

func (j *Job) dependsOn(parents ...*Job) {
	j.depends = append(j.depends, parents...)
	for _, p := range parents {
		j.Upstreams = append(j.Upstreams, p)
	}
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
	Name           string
	Schedule       string
	Config         map[string]any
	Jobs           []*Job
	xcom           map[string]any
	xcomLock       sync.Mutex
	wg             sync.WaitGroup
	jobMap         map[string]*Job
	logFile        *os.File
	logLock        sync.Mutex
	activeJobs     map[string]bool
	activeJobsLock sync.Mutex // Mutex untuk melindungi map activeJobs
	Connections    map[string]*Connection
	isRunning      bool
	runningLock    sync.Mutex
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
			if upstream.getStatus() != JobSuccess {
				return false
			}
		}
		return true
	case AllFailed:
		for _, upstream := range job.Upstreams {
			if upstream.getStatus() != JobFailed {
				return false
			}
		}
		return true
	case Always:
		return true
	default:
		for _, upstream := range job.Upstreams {
			if upstream.getStatus() != JobSuccess { // Menggunakan getStatus()
				return false
			}
		}
		return true
	}
}

func (d *DAG) trySetRunning() bool {
	d.runningLock.Lock()
	defer d.runningLock.Unlock()
	if d.isRunning {
		return false
	}
	d.isRunning = true
	return true
}

func (d *DAG) unsetRunning() {
	d.runningLock.Lock()
	defer d.runningLock.Unlock()
	d.isRunning = false
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

func (d *DAG) NewJob(id string, action func(ctx *Context)) *Job {
	job := &Job{
		ID:          id,
		action:      action,
		status:      JobPending,
		TriggerRule: AllSuccess,
	}
	d.Jobs = append(d.Jobs, job)
	d.jobMap[id] = job
	return job
}

func (d *DAG) NewBranchJob(id string, branchFunc func(ctx *Context) []string) *Job {
	job := &Job{ID: id, branchFunc: branchFunc, status: JobPending} // Menggunakan konstanta JobPending
	d.Jobs = append(d.Jobs, job)
	d.jobMap[id] = job
	return job
}

func (d *DAG) NewBigQueryJob(id string, queryPath string, params map[string]any) *Job {
	op := &operator.BigQueryOperator{
		TaskID:    id,
		Logf:      d.Logf,
		LogErrorf: d.LogErrorf,
	}

	job := d.NewJob(id, func(ctx *Context) {
		raw, err := os.ReadFile(queryPath)
		if err != nil {
			d.LogErrorf("Failed to read query file %s: %v", queryPath, err)
			panic(err)
		}

		funcMap := template.FuncMap{
			"xcom": func(key string) string {
				val := ctx.GetXCom(key)
				if str, ok := val.(string); ok {
					return str
				}
				return ""
			},
		}

		// Parse & render template SQL
		tmpl, err := template.New("bq_query").Funcs(funcMap).Parse(string(raw))
		if err != nil {
			d.LogErrorf("Failed to parse query template: %v", err)
			panic(err)
		}

		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, params); err != nil {
			d.LogErrorf("Failed to render query template: %v", err)
			panic(err)
		}
		op.Query = buf.String()

		// Render project_id
		projectID := ""
		if val, ok := params["project_id"].(string); ok && val != "" {
			var bufID bytes.Buffer
			tmplID, _ := template.New("project_id").Funcs(funcMap).Parse(val)
			_ = tmplID.Execute(&bufID, params)
			projectID = bufID.String()
		} else if val, ok := ctx.GetXCom("project_id").(string); ok {
			projectID = val
		} else if val, ok := d.Config["project_id"].(string); ok {
			projectID = val
		} else if conn, ok := d.Connections["env_bigquery"]; ok {
			if val, ok := conn.Config["project_id"].(string); ok {
				projectID = val
			}
		}

		op.ProjectID = projectID

		if err := op.Execute(); err != nil {
			panic(err)
		}
	})

	return job
}


func (d *DAG) markJobActive(j *Job) {
	d.activeJobsLock.Lock()
	defer d.activeJobsLock.Unlock()
	if !d.activeJobs[j.ID] {
		d.activeJobs[j.ID] = true
	}
}

func (d *DAG) recoverFromPanic(j *Job) {
	if r := recover(); r != nil {
		j.setStatus(JobFailed)
		d.LogErrorf("Job %s panic: %v", j.ID, r)
	}
}

func (d *DAG) runActionJob(j *Job, ctx *Context) {
	j.action(ctx)
	j.setStatus(JobSuccess)
	d.Logf("Completed job %s", j.ID)
}

func (d *DAG) runBranchJob(j *Job, ctx *Context, jobWg *sync.WaitGroup, dagCtx context.Context) {
	selected := j.branchFunc(ctx)
	d.Logf("Branch %s selected %v", j.ID, selected)

	j.setStatus(JobSuccess)

	for _, job := range d.Jobs {
		if job == nil || job.branchFunc != nil {
			continue
		}
		if isBranchChild(job) && !contains(selected, job.ID) {
			d.Logf("Skipping job %s (auto-success to unblock DAG)", job.ID)
			job.setStatus(JobSkipped)
		}
	}

	for _, selID := range selected {
		if child, ok := d.jobMap[selID]; ok {
			jobWg.Add(1)
			go d.runJobWithContext(child, ctx, jobWg, dagCtx)
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
	d.activeJobsLock.Lock()
	defer d.activeJobsLock.Unlock()

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
	d.activeJobsLock.Lock()
	d.activeJobs = make(map[string]bool) // Reset active jobs
	d.activeJobsLock.Unlock()

	for _, job := range d.Jobs {
		job.setStatus(JobPending)
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
		target.setStatus(JobPending)
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
	job.setStatus(JobPending)
	for _, dep := range job.depends {
		d.resetUpstream(dep, visited)
	}
}

func (d *DAG) resetDownstream(job *Job, visited map[string]bool) {
	if visited[job.ID] {
		return
	}
	visited[job.ID] = true
	job.setStatus(JobPending)
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
