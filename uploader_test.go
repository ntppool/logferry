package main

import "testing"

/*
File Validation Tests (uploader_test.go)

WHAT IS BEING TESTED:
- Basic validation: Files must end with ".avro" and have content before extension
- Path handling: Files with paths (e.g., "path/to/data.avro") are accepted
- Case sensitivity: Only lowercase ".avro" is accepted
- Extension isolation: ".avro" in middle or start of filename doesn't count
- Special characters: Hyphens, underscores, numbers in filenames are allowed
- Edge cases: Empty strings, very long filenames, multiple dots
- Whitespace handling: Filenames with leading/trailing whitespace are rejected

WHAT IS NOT BEING TESTED:
- File existence on disk
- File readability/permissions
- File content validation
- Unicode filename support
- Network path handling (UNC paths, etc.)
- Path traversal protection

NOTES:
- ".avro" alone is NOT a valid filename - requires at least one character before extension

QUESTIONS:
- Should Unicode characters in filenames be supported? (e.g., "データ.avro")
- What about case-insensitive filesystems? Should "DATA.AVRO" be accepted on Windows?
- Should there be any path traversal validation (e.g., reject "../../../etc/passwd.avro")?
*/

// TestIsAvroFileName tests core filename validation logic
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
		{"only extension", ".avro", false},
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

// TestIsAvroFileName_EdgeCases tests boundary conditions and unusual inputs
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
	if isAvroFileName(".avro") {
		t.Error("Should not accept just .avro")
	}

	// Test with multiple .avro occurrences
	if isAvroFileName("test.avro.notavro") {
		t.Error("Should not accept files that don't end with .avro")
	}
}
