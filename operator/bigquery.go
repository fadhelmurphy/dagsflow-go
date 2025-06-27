package operator

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"time"
)

type BigQueryOperator struct {
	TaskID    string
	Query     string
	ProjectID string
	Logf      func(format string, args ...interface{})
	LogErrorf func(format string, args ...interface{})
}

func (op *BigQueryOperator) Execute() error {
	if op.ProjectID == "" {
		err := fmt.Errorf("project ID is empty")
		op.LogErrorf("[BigQueryOperator %s] %v", op.TaskID, err)
		return err
	}

	if op.Query == "" {
		err := fmt.Errorf("query is empty")
		op.LogErrorf("[BigQueryOperator %s] %v", op.TaskID, err)
		return err
	}

	op.Logf("[BigQueryOperator %s] Running query on project %s", op.TaskID, op.ProjectID)
	op.Logf("[BigQueryOperator %s] Query:\n%s", op.TaskID, op.Query)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	client, err := bigquery.NewClient(ctx, op.ProjectID)
	if err != nil {
		op.LogErrorf("[BigQueryOperator %s] Failed to create client: %v", op.TaskID, err)
		return fmt.Errorf("failed to create BQ client: %w", err)
	}
	defer client.Close()

	q := client.Query(op.Query)
	job, err := q.Run(ctx)
	if err != nil {
		op.LogErrorf("[BigQueryOperator %s] Failed to start query: %v", op.TaskID, err)
		return fmt.Errorf("failed to start BQ query: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		op.LogErrorf("[BigQueryOperator %s] Query wait failed: %v", op.TaskID, err)
		return fmt.Errorf("query wait failed: %w", err)
	}

	if err := status.Err(); err != nil {
		op.LogErrorf("[BigQueryOperator %s] Query execution error: %v", op.TaskID, err)
		return fmt.Errorf("query execution error: %w", err)
	}

	op.Logf("[BigQueryOperator %s] Query executed successfully at %v", op.TaskID, time.Now())
	return nil
}
