package cmd

import (
	"reflect"
	"strings"
	"testing"
)

func TestStatedb2Factory_parseReader(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected []diff
	}{
		{
			name:  "valid input",
			input: "not found: ns namespace1 key 123456\nunmatch: ns namespace2 key abcdef",
			expected: []diff{
				{ns: "namespace1", key: []byte{0x12, 0x34, 0x56}},
				{ns: "namespace2", key: []byte{0xab, 0xcd, 0xef}},
			},
		},
		{
			name:     "empty input",
			input:    "",
			expected: nil,
		},
		{
			name:     "other input",
			input:    "abcdefdg",
			expected: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader := strings.NewReader(tc.input)
			result, err := parseReader(reader)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}
