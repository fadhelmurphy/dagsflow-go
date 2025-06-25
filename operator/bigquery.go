package operator

import (
	"cloud.google.com/go/bigquery"
	"context"
	"time"
)

type BigQueryOperator struct {
	TaskID    string
	Query     string
	ProjectID string
	Logf      func(format string, args ...interface{})
	LogErrorf func(format string, args ...interface{})
}

func (op *BigQueryOperator) Execute() {
	if op.ProjectID == "" {
		op.LogErrorf("[BigQueryOperator %s] Project ID is empty", op.TaskID)
		return
	}
	if op.Query == "" {
		op.LogErrorf("[BigQueryOperator %s] Query is empty", op.TaskID)
		return
	}

	op.Logf("[BigQueryOperator %s] Running query on project %s", op.TaskID, op.ProjectID)
	op.Logf("[BigQueryOperator %s] Query:\n%s", op.TaskID, op.Query)

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, op.ProjectID)
	if err != nil {
		op.LogErrorf("[BigQueryOperator %s] Failed to create client: %v", op.TaskID, err)
		return
	}
	defer client.Close()

	q := client.Query(op.Query)
	job, err := q.Run(ctx)
	if err != nil {
		op.LogErrorf("[BigQueryOperator %s] Failed to start query: %v", op.TaskID, err)
		return
	}

	status, err := job.Wait(ctx)
	if err != nil {
		op.LogErrorf("[BigQueryOperator %s] Query wait failed: %v", op.TaskID, err)
		return
	}

	if err := status.Err(); err != nil {
		op.LogErrorf("[BigQueryOperator %s] Query execution error: %v", op.TaskID, err)
		return
	}

	op.Logf("[BigQueryOperator %s] Query executed successfully at %v", op.TaskID, time.Now())
}
