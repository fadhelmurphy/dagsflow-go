package dags

import (
	"fmt"
	"dagsflow-go/dag"
)

func init() {
	d := dag.NewDAG("dag1", "*/1 * * * *")
	a := d.NewJob("a", func(ctx *dag.Context) {
		ctx.DAG.Logf("[DAG1] Run A PAKE LOGF")
		fmt.Println("[DAG1] Run A")
		ctx.SetXCom("sebuah-key", "sebuah-nilai")

	})
	b := d.NewJob("b", func(ctx *dag.Context) {
		getKey := ctx.GetXCom("sebuah-key")
		logGetKey := fmt.Sprintf("%s << ini nilai dari key", getKey)
		ctx.DAG.Logf(logGetKey)
		ctx.DAG.Logf("[DAG1] Run B")
	})
	a.Then(b)
	dag.Register(d)
}
