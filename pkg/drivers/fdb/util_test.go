package fdb

import "testing"

func TestExtractResourceType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"/bootstrap", "default"},
		{"/registry/secrets/", "secrets"},
		{"/registry/pods/", "pods"},
		{"/registry/pods", "pods"},
		{"/registry/pods/default/SVqMa", "pods"},
		{"/registry/health", "health"},
		{"/registry/ranges/serviceips", "ranges"},
		{"/registry/podtemplates/chunking-6414/template-0016", "podtemplates"},
		{"/registry/masterleases/172.17.0.2", "masterleases"},
		{"/registry/clusterroles/system:aggregate-to-edit", "clusterroles"},
		{"/unknown", "default"}, // fallback behavior
		{"/", "default"},        // edge case: only slash
		{"", "default"},         // edge case: empty string
	}

	for _, tt := range tests {
		got := extractResourceType(tt.input)
		if got != tt.expected {
			t.Errorf("TestExtractResourceType(%q) = %q; want %q", tt.input, got, tt.expected)
		}
	}
}
