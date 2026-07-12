package dag

import (
	"testing"

	"github.com/aman0603/flowforge/internal/model"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		req     model.CreateDefinitionRequest
		wantErr bool
	}{
		{
			name: "Valid linear graph",
			req: model.CreateDefinitionRequest{
				Name: "Linear",
				Tasks: []model.TaskDefinitionInput{
					{Name: "TaskA"},
					{Name: "TaskB", Dependencies: []string{"TaskA"}},
					{Name: "TaskC", Dependencies: []string{"TaskB"}},
				},
			},
			wantErr: false,
		},
		{
			name: "Valid diamond graph",
			req: model.CreateDefinitionRequest{
				Name: "Diamond",
				Tasks: []model.TaskDefinitionInput{
					{Name: "TaskA"},
					{Name: "TaskB", Dependencies: []string{"TaskA"}},
					{Name: "TaskC", Dependencies: []string{"TaskA"}},
					{Name: "TaskD", Dependencies: []string{"TaskB", "TaskC"}},
				},
			},
			wantErr: false,
		},
		{
			name: "Invalid - empty task list",
			req: model.CreateDefinitionRequest{
				Name:  "Empty",
				Tasks: []model.TaskDefinitionInput{},
			},
			wantErr: true,
		},
		{
			name: "Invalid - empty task name",
			req: model.CreateDefinitionRequest{
				Name: "EmptyName",
				Tasks: []model.TaskDefinitionInput{
					{Name: ""},
				},
			},
			wantErr: true,
		},
		{
			name: "Invalid - duplicate task name",
			req: model.CreateDefinitionRequest{
				Name: "Duplicate",
				Tasks: []model.TaskDefinitionInput{
					{Name: "TaskA"},
					{Name: "TaskA"},
				},
			},
			wantErr: true,
		},
		{
			name: "Invalid - missing dependency",
			req: model.CreateDefinitionRequest{
				Name: "MissingDep",
				Tasks: []model.TaskDefinitionInput{
					{Name: "TaskA", Dependencies: []string{"NonExistent"}},
				},
			},
			wantErr: true,
		},
		{
			name: "Invalid - direct cycle",
			req: model.CreateDefinitionRequest{
				Name: "DirectCycle",
				Tasks: []model.TaskDefinitionInput{
					{Name: "TaskA", Dependencies: []string{"TaskA"}},
				},
			},
			wantErr: true,
		},
		{
			name: "Invalid - simple indirect cycle",
			req: model.CreateDefinitionRequest{
				Name: "IndirectCycle",
				Tasks: []model.TaskDefinitionInput{
					{Name: "TaskA", Dependencies: []string{"TaskB"}},
					{Name: "TaskB", Dependencies: []string{"TaskA"}},
				},
			},
			wantErr: true,
		},
		{
			name: "Invalid - long cycle",
			req: model.CreateDefinitionRequest{
				Name: "LongCycle",
				Tasks: []model.TaskDefinitionInput{
					{Name: "TaskA", Dependencies: []string{"TaskC"}},
					{Name: "TaskB", Dependencies: []string{"TaskA"}},
					{Name: "TaskC", Dependencies: []string{"TaskB"}},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(&tt.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
