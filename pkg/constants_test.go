package pkg

import "testing"

func TestBuildPriorityClass(t *testing.T) {
	tests := []struct {
		priority string
		want     string
	}{
		{Highest, "whitelist"},
		{Urgent, "urgent"},
		{High, "high"},
		{Medium, "medium"},
		{Low, "low"},
		{"unknown", "low"},
		{"", "low"},
	}
	for _, tt := range tests {
		t.Run(tt.priority, func(t *testing.T) {
			if got := BuildPriorityClass(tt.priority); got != tt.want {
				t.Fatalf("BuildPriorityClass(%q) = %q, want %q", tt.priority, got, tt.want)
			}
		})
	}
}

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
