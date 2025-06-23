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
