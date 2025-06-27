package dags

import (
	"fmt"
	"dagsflow-go/dag"
	"strconv"
)

func init() {
	d := dag.NewDAG("dag1", "@every 1s")
	a := d.NewJob("a", func(ctx *dag.Context) {
		ctx.DAG.Logf("[DAG1] Run A PAKE LOGF")
		fmt.Println("[DAG1] Run A")
		ctx.SetXCom("sebuah-key", "sebuah-nilai")
		panic("something went wrong!")

	})
	b := d.NewJob("b", func(ctx *dag.Context) {
		
		getParam2 := ctx.DAG.Config["param2"].(int)
		ctx.DAG.Logf("%s << getParam2", strconv.Itoa(getParam2))
		getKey := ctx.GetXCom("sebuah-key")
		logGetKey := fmt.Sprintf("%s << ini nilai dari key", getKey)
		ctx.DAG.Logf(logGetKey)
		ctx.DAG.Logf("[DAG1] Run B")
	})
	a.Then(b)
	dag.Register(d)
}
