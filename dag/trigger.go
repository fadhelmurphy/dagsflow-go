package dag

import (
	"context"
)

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