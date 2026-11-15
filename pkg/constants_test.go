package pkg

import "testing"

func TestJobPriorityRank(t *testing.T) {
	tests := []struct {
		priority string
		want     int
	}{
		{Highest, 5},
		{Urgent, 4},
		{High, 3},
		{Medium, 2},
		{Low, 1},
		{"unknown", 1},
		{"", 1},
	}
	for _, tt := range tests {
		t.Run(tt.priority, func(t *testing.T) {
			if got := JobPriorityRank(tt.priority); got != tt.want {
				t.Fatalf("JobPriorityRank(%q) = %d, want %d", tt.priority, got, tt.want)
			}
		})
	}
}
