package dags

import (
	"dagsflow-go/dag"
)

func init() {
	d := dag.NewDAG("jinja_dag", "@every 1s")

		setXcomJob := d.NewJob("set_xcom", func(ctx *dag.Context) {
		ctx.SetXCom("country", "Indonesia")
		ctx.SetXCom("username", "Fadhel")
	})

	// sqlJob := d.NewSQLJob("run_sql", "query.sql")
	shJob := d.NewShellJob("run_shell", "dags/script.sh")

	setXcomJob.Then(shJob)
	d.Run()
}