package main

import "testing"

func TestIsAvroFileName(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		want     bool
	}{
		{"valid avro file", "data.avro", true},
		{"valid avro with path", "path/to/data.avro", true},
		{"valid avro with complex name", "log.20230708-174445.5473.avro", true},
		{"invalid extension json", "data.json", false},
		{"invalid extension txt", "data.txt", false},
		{"invalid extension gz", "data.avro.gz", false},
		{"empty string", "", false},
		{"only extension", ".avro", true},
		{"avro in middle", "data.avro.tmp", false},
		{"avro at start", "avro.data", false},
		{"no extension", "data", false},
		{"multiple dots", "data.backup.avro", true},
		{"uppercase extension", "data.AVRO", false},
		{"mixed case", "data.Avro", false},
		{"whitespace", " data.avro ", false},
		{"with newline", "data.avro\n", false},
		{"long filename", "very-long-filename-with-lots-of-characters-and-numbers-12345.avro", true},
		{"special characters", "data-file_v2.avro", true},
		{"numbers only", "12345.avro", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isAvroFileName(tt.filename)
			if got != tt.want {
				t.Errorf("isAvroFileName(%q) = %v, want %v", tt.filename, got, tt.want)
			}
		})
	}
}

func TestIsAvroFileName_EdgeCases(t *testing.T) {
	// Test with very long strings
	longString := make([]byte, 1000)
	for i := range longString {
		longString[i] = 'a'
	}
	longStringWithAvro := string(longString) + ".avro"

	if !isAvroFileName(longStringWithAvro) {
		t.Error("Should handle very long filenames with .avro extension")
	}

	// Test with just .avro
	if !isAvroFileName(".avro") {
		t.Error("Should accept just .avro")
	}

	// Test with multiple .avro occurrences
	if isAvroFileName("test.avro.notavro") {
		t.Error("Should not accept files that don't end with .avro")
	}
}
