package dag

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"dagsflow-go/operator"
	"os"
	"path/filepath"
	"sync"
	"time"
	"text/template"

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
	statusLock  sync.Mutex // Mutex for Job status
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
	// Untuk menghindari deadlock, getStatus() menggunakan mutex internalnya sendiri.
	// Kita tidak perlu mengunci activeJobsLock di sini karena kita hanya membaca status job
	// yang dilindungi oleh mutex job masing-masing.
	switch job.TriggerRule {
	case AllSuccess:
		for _, upstream := range job.Upstreams {
			if upstream.getStatus() != JobSuccess { // Menggunakan getStatus()
				return false
			}
		}
		return true
	case AllFailed:
		for _, upstream := range job.Upstreams {
			if upstream.getStatus() != JobFailed { // Menggunakan getStatus()
				return false
			}
		}
		return true
	case Always:
		return true
	default: // Default trigger rule is AllSuccess
		for _, upstream := range job.Upstreams {
			if upstream.getStatus() != JobSuccess { // Menggunakan getStatus()
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
	// d.logFile.Sync() // Tidak perlu Sync setiap baris, bisa jadi overhead. Sync jika penting.
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
		Name:           name,
		Schedule:       schedule,
		Config:         cfg,
		xcom:           make(map[string]interface{}),
		jobMap:         make(map[string]*Job),
		activeJobs:     make(map[string]bool),
		Connections:    make(map[string]*Connection),
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

		// Hati-hati mengubah config secara langsung jika target.Config juga diakses
		// oleh goroutine lain tanpa proteksi. Namun, dalam konteks trigger,
		// biasanya ini adalah inisialisasi awal.
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
		ID:          id,
		action:      action,
		status:      JobPending, // Menggunakan konstanta JobPending
		TriggerRule: Always,
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
			panic(err) // ← trigger panic untuk deteksi gagal
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
		// Reset activeJobs for a new cron run
		d.activeJobsLock.Lock()
		d.activeJobs = make(map[string]bool) 
		d.activeJobsLock.Unlock()
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
	// Inisialisasi atau reset activeJobs di awal setiap runDAGWithContext
	// Ini penting agar setiap eksekusi DAG dimulai dengan daftar job aktif yang bersih.
	d.activeJobsLock.Lock()
	d.activeJobs = make(map[string]bool)
	d.activeJobsLock.Unlock()
	d.Logf("Running DAG: %s", d.Name)

	myCtx := &Context{DAG: d}
	var jobWg sync.WaitGroup

	for _, job := range d.Jobs {
		job.setStatus(JobPending) // Reset status job ke PENDING untuk setiap run
	}

	for _, job := range d.Jobs {
		if len(job.depends) == 0 && d.shouldRun(job) {
			jobWg.Add(1)
			go d.runJobWithContext(job, myCtx, &jobWg, ctx)
		}
	}

	jobWg.Wait()
	d.Logf("DAG %s finished.", d.Name)
}

func (d *DAG) waitUntilParentsDone(j *Job, dagCtx context.Context) bool {
	for {
		allDone := true
		for _, dep := range j.depends {
			status := dep.getStatus() // Menggunakan getStatus()
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

func (d *DAG) markJobActive(j *Job) {
	d.activeJobsLock.Lock()   // Kunci map activeJobs
	defer d.activeJobsLock.Unlock() // Pastikan kunci dilepaskan

	if !d.activeJobs[j.ID] {
		d.activeJobs[j.ID] = true
	}
}

func (d *DAG) recoverFromPanic(j *Job) {
	if r := recover(); r != nil {
		j.setStatus(JobFailed) // Menggunakan setStatus()
		d.LogErrorf("Job %s panic: %v", j.ID, r)
	}
}

func (d *DAG) runActionJob(j *Job, ctx *Context) {
	j.action(ctx)
	j.setStatus(JobSuccess) // Menggunakan setStatus()
	d.Logf("Completed job %s", j.ID)
}

func (d *DAG) runBranchJob(j *Job, ctx *Context, jobWg *sync.WaitGroup, dagCtx context.Context) {
	selected := j.branchFunc(ctx)
	d.Logf("Branch %s selected %v", j.ID, selected)

	// Mark downstream active for selected branches
	for _, selID := range selected {
		d.markDownstreamActive(selID)
	}

	// Skip yang tidak dipilih
	for _, job := range d.Jobs {
		if job == nil || job.branchFunc != nil {
			continue
		}
		if isBranchChild(job) && !contains(selected, job.ID) {
			d.Logf("Skipping job %s (auto-success to unblock DAG)", job.ID)
			job.setStatus(JobSkipped) // Menggunakan setStatus()
		}
	}

	j.setStatus(JobSuccess) // Menggunakan setStatus()

	// Jalankan yang dipilih
	for _, selID := range selected {
		if child, ok := d.jobMap[selID]; ok {
			jobWg.Add(1)
			go d.runJobWithContext(child, ctx, jobWg, dagCtx)
		}
	}
}

func (d *DAG) triggerChildrenIfReady(j *Job, ctx *Context, jobWg *sync.WaitGroup, dagCtx context.Context) {
	for _, child := range d.getChildren(j) {
		if d.shouldRun(child) {
			jobWg.Add(1)
			go d.runJobWithContext(child, ctx, jobWg, dagCtx)
		} else {
			child.setStatus(JobSkipped) // Menggunakan setStatus()
			d.Logf("Skipping job %s due to unmet trigger rule", child.ID)
		}
	}
}

func (d *DAG) runJobWithContext(j *Job, ctx *Context, jobWg *sync.WaitGroup, dagCtx context.Context) {
	defer jobWg.Done()

	// Tunggu semua parent selesai (SUCCESS, FAILED, SKIPPED)
	if !d.waitUntilParentsDone(j, dagCtx) {
		return // dibatalkan
	}

	// Evaluasi Trigger Rule
	if !d.shouldRun(j) {
		j.setStatus(JobSkipped) // Menggunakan setStatus()
		d.Logf("Skipping job %s due to unmet trigger rule", j.ID)
		return
	}

	d.markJobActive(j)

	// Set status & eksekusi
	j.setStatus(JobRunning) // Menggunakan setStatus()
	d.Logf("Starting job %s", j.ID)

	defer d.recoverFromPanic(j)

	if j.action != nil {
		d.runActionJob(j, ctx)
	} else if j.branchFunc != nil {
		d.runBranchJob(j, ctx, jobWg, dagCtx)
		return
	} else {
		j.setStatus(JobSuccess) // Menggunakan setStatus()
		d.Logf("Completed job %s", j.ID)
	}

	d.triggerChildrenIfReady(j, ctx, jobWg, dagCtx)
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
	d.activeJobsLock.Lock()   // Kunci map activeJobs
	defer d.activeJobsLock.Unlock() // Pastikan kunci dilepaskan

	if d.activeJobs[jobID] {
		return
	}
	d.activeJobs[jobID] = true

	if job, ok := d.jobMap[jobID]; ok {
		for _, child := range d.getChildren(job) {
			// Perhatikan: Rekursi ini juga akan mengunci mutex di setiap panggilan,
			// yang seharusnya aman karena mutex melindungi seluruh map.
			d.markDownstreamActive(child.ID) 
		}
	}
}

func (d *DAG) RerunDAG() {
	d.Logf("Rerunning DAG: %s", d.Name)
	// Reset status semua job ke PENDING dan reset activeJobs
	d.activeJobsLock.Lock()
	d.activeJobs = make(map[string]bool) // Reset active jobs
	d.activeJobsLock.Unlock()

	for _, job := range d.Jobs {
		job.setStatus(JobPending) // Menggunakan setStatus()
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
		target.setStatus(JobPending) // Menggunakan setStatus()
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
	job.setStatus(JobPending) // Menggunakan setStatus()
	for _, dep := range job.depends {
		d.resetUpstream(dep, visited)
	}
}

func (d *DAG) resetDownstream(job *Job, visited map[string]bool) {
	if visited[job.ID] {
		return
	}
	visited[job.ID] = true
	job.setStatus(JobPending) // Menggunakan setStatus()
	for _, child := range d.getChildren(job) {
		d.resetDownstream(child, visited)
	}
}

// getStatus dan setStatus sudah menggunakan mutex internal Job, jadi aman.
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
	// Visited map lokal untuk PrintGraph agar tidak mengganggu state DAG
	visited := make(map[string]bool) 
	for _, job := range d.Jobs {
		if len(job.depends) == 0 {
			d.printJobTree(job, "", true, visited)
		}
	}
}

func (d *DAG) printJobTree(j *Job, prefix string, isLast bool, visited map[string]bool) {
	// Hindari cetak ulang node yang sudah dikunjungi (untuk DAG dengan diamond structure)
	if visited[j.ID] {
        return
    }
    visited[j.ID] = true

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