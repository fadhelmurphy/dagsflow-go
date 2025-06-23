# dagsflow-go

**dagsflow-go** adalah tools CLI sederhana untuk membuat dan menjalankan DAG scheduler mirip seperti Airflow menggunakan Go.

---

## Fitur
- Schedule berbasis cron (`robfig/cron`)
- Support DAG dengan dependency dan branching
- Detach process (background)
- List status DAG (Is Running / Schedule)
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
