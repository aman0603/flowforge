package dag

import (
	"fmt"

	"github.com/aman0603/flowforge/internal/model"
)

// Validate checks if a workflow definition is structurally valid.
// It verifies that there are tasks, names are unique, dependencies reference existing tasks,
// and there are no cycles (is a valid DAG).
func Validate(req *model.CreateDefinitionRequest) error {
	if len(req.Tasks) == 0 {
		return fmt.Errorf("workflow must contain at least one task")
	}

	// Step 1: Map task names to check for duplicates and ensure dependencies are valid
	taskMap := make(map[string]bool)
	for _, t := range req.Tasks {
		if t.Name == "" {
			return fmt.Errorf("task name cannot be empty")
		}
		if taskMap[t.Name] {
			return fmt.Errorf("duplicate task name: %s", t.Name)
		}
		taskMap[t.Name] = true
	}

	// Step 2: Build the adjacency list representation of the graph
	// We map: taskName -> list of child tasks (tasks that depend on taskName)
	// We also verify that all listed dependencies exist.
	adj := make(map[string][]string)

	// Track all tasks that have dependencies to initialize the adjacency list for all nodes
	for _, t := range req.Tasks {
		if _, exists := adj[t.Name]; !exists {
			adj[t.Name] = []string{}
		}
		for _, dep := range t.Dependencies {
			if dep == t.Name {
				return fmt.Errorf("task %q cannot depend on itself", t.Name)
			}
			if !taskMap[dep] {
				return fmt.Errorf("task %q depends on non-existent task %q", t.Name, dep)
			}
			// dep is a parent, t.Name is a child: edge is dep -> t.Name
			adj[dep] = append(adj[dep], t.Name)
		}
	}

	// Step 3: Run Cycle Detection (DFS with 3-color marking)
	// state: 0 = unvisited, 1 = visiting, 2 = visited
	state := make(map[string]int)

	var hasCycle func(node string) bool
	hasCycle = func(node string) bool {
		state[node] = 1 // Mark as visiting

		for _, neighbor := range adj[node] {
			if state[neighbor] == 1 {
				return true // Cycle detected
			}
			if state[neighbor] == 0 {
				if hasCycle(neighbor) {
					return true
				}
			}
		}

		state[node] = 2 // Mark as visited
		return false
	}

	for _, t := range req.Tasks {
		if state[t.Name] == 0 {
			if hasCycle(t.Name) {
				return fmt.Errorf("circular dependency detected in workflow")
			}
		}
	}

	return nil
}
