# dagsflow-go

**dagsflow-go** adalah tools CLI sederhana untuk membuat dan menjalankan DAG scheduler mirip seperti Airflow menggunakan Go.

---

## Fitur
- Schedule berbasis cron (`robfig/cron`)
- Support DAG dengan dependency dan branching
- Detach process (background)
- List status DAG (Is Running / Schedule)
- Rerun DAG / job (dengan upstream / downstream)
- Trigger DAG lain (blocking / non-blocking)
- Configurable DAG (mirip Airflow)
- Cross-platform tanpa HTTP server / REST API

---

## Instalasi

```bash
git clone <repo-url>
cd dagsflow-go
go build -o dagsflow-go
```

## Cara Pakai

### Jalankan 1 DAG

```bash
./dagsflow-go run <dagName>
```

### Jalankan semua DAG

```bash
./dagsflow-go run-all
```

### Stop 1 DAG

```bash
./dagsflow-go stop <dagName>
```

### Stop semua DAG

```bash
./dagsflow-go stop-all
```

### Lihat status DAG

```bash
./dagsflow-go list
```

```bash
DAG Name        | Is Running | Schedule
----------------|------------|---------------------
dag1            | true       | */1 * * * *
dag2            | false      | */2 * * * *
```

### Lihat Graph DAG

```bash
./dagsflow-go graph <dagName>
```

## Struktur PID / Marker

- PID file: dagsflow-pid/{dagName}.pid

- Running marker: dagsflow-pid/{dagName}.running

## Contoh DAG

```golang
package dags

import (
	"fmt"
	"dagsflow-go/dag"
)

func init() {
	d := dag.NewDAG("dag1", "*/1 * * * *")
	a := d.NewJob("a", func(ctx *dag.Context) {
		fmt.Println("[DAG1] Run A")
	})
	b := d.NewJob("b", func(ctx *dag.Context) {
		fmt.Println("[DAG1] Run B")
	})
	a.Then(b)
	dag.Register(d)
}
```

## Rerun DAG
```bash
./dagsflow-go rerun-dag <dagName>
```
## Rerun Job
```bash
./dagsflow-go rerun-job <dagName> <jobID>
```
### Rerun Job dengan upstream / downstream

```bash
./dagsflow-go rerun-job <dagName> <jobID> --upstream
./dagsflow-go rerun-job <dagName> <jobID> --downstream
./dagsflow-go rerun-job <dagName> <jobID> --upstream --downstream
```

## Config

```golang
d := dag.NewDAG("custom_dag", "*/1 * * * *", map[string]interface{}{
	"threshold": 50,
	"region":    "APAC",
})
```
di dalam job confignya dapat diakses :

```golang
val := ctx.DAG.Config["threshold"].(int)
fmt.Println("Threshold is", val)

```

## Trigger DAG lain sebagai job

### Non-blocking trigger
```golang
	triggerJob := d.NewJob("trigger_dag_branch", func(ctx *dag.Context) {
	config := map[string]interface{}{
		"param1": "value1",
		"param2": 42,
	}

	// ctx.DAG.TriggerDAG("dag_branch") // tanpa config
	ctx.DAG.TriggerDAGWithConfig("dag_branch",config, false) // dengan config 
	}) // Non Blocking example (tidak perlu nunggu dag nya kelar)

```

### Blocking trigger
```golang
	triggerBlockingJob := d.NewJob("trigger_dag1", func(ctx *dag.Context) {
			config := map[string]any{
		"param1": "value1",
		"param2": 42,
	}
	// ctx.DAG.TriggerDAGBlocking("dag1") // tanpa config
	ctx.DAG.TriggerDAGWithConfig("dag1", config, true) // dengan config 
	}) // Blocking example (perlu nunggu dag nya kelar)

```