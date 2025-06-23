package dags

import (
	"fmt"
	"dagsflow-go/dag"
)

func init() {
	d := dag.NewDAG("dag2", "*/2 * * * *")
	x := d.NewJob("x", func(ctx *dag.Context) {
		fmt.Println("[DAG2] Run 2")
		ctx.DAG.Logf("[DAG2] Run 2")
		ctx.SetXCom("key", "value")
	})
	y := d.NewJob("y", func(ctx *dag.Context) {
		fmt.Println(ctx.GetXCom("key"), "<< ini nilai dari key")
		fmt.Println("[DAG2] Run Y")
	})
	x.Then(y)
	dag.Register(d)
}
