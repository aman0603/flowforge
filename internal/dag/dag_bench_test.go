package dag

import (
	"fmt"
	"testing"

	"github.com/aman0603/flowforge/internal/model"
)

// buildChain returns a linear DAG of n tasks where each task depends on the
// previous one — a worst-case depth for the DFS cycle detector.
func buildChain(n int) *model.CreateDefinitionRequest {
	tasks := make([]model.TaskDefinitionInput, n)
	for i := 0; i < n; i++ {
		t := model.TaskDefinitionInput{Name: fmt.Sprintf("t%d", i)}
		if i > 0 {
			t.Dependencies = []string{fmt.Sprintf("t%d", i-1)}
		}
		tasks[i] = t
	}
	return &model.CreateDefinitionRequest{Tasks: tasks}
}

// buildWide returns a DAG with one root and n-1 leaves depending on it.
func buildWide(n int) *model.CreateDefinitionRequest {
	tasks := make([]model.TaskDefinitionInput, n)
	tasks[0] = model.TaskDefinitionInput{Name: "root"}
	for i := 1; i < n; i++ {
		tasks[i] = model.TaskDefinitionInput{
			Name:         fmt.Sprintf("leaf%d", i),
			Dependencies: []string{"root"},
		}
	}
	return &model.CreateDefinitionRequest{Tasks: tasks}
}

func BenchmarkValidateChain(b *testing.B) {
	for _, n := range []int{10, 100, 500} {
		req := buildChain(n)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := Validate(req); err != nil {
					b.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

func BenchmarkValidateWide(b *testing.B) {
	for _, n := range []int{10, 100, 500} {
		req := buildWide(n)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := Validate(req); err != nil {
					b.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}
