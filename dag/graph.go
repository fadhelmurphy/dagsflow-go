package dag

import (
	"fmt"
)

func (d *DAG) printJobTree(j *Job, prefix string, isLast bool, visited map[string]bool, parentID string) {
	connector := "├──"
	newPrefix := prefix + "│   "
	if isLast {
		connector = "└──"
		newPrefix = prefix + "    "
	}

	label := ""
	if parentID != "" {
		label = fmt.Sprintf("\x1b[3m\x1b[38;5;245m [from %s]\x1b[0m", parentID)

	}

	fmt.Printf("%s%s %s%s\n", prefix, connector, j.ID, label)

	if visited[j.ID] {
		return
	}
	visited[j.ID] = true

	children := d.getChildren(j)
	for i, child := range children {
		last := i == len(children)-1
		d.printJobTree(child, newPrefix, last, visited, j.ID)
	}
}

func (d *DAG) getChildren(j *Job) []*Job {
	var children []*Job
	for _, job := range d.Jobs {
		for _, dep := range job.depends {
			if dep == j {
				children = append(children, job)
			}
		}
	}
	return children
}

func (d *DAG) PrintGraph() {
	fmt.Printf("DAG: %s\n", d.Name)
	visited := make(map[string]bool)
	for _, job := range d.Jobs {
		if len(job.depends) == 0 {
			d.printJobTree(job, "", true, visited, "")
		}
	}
}