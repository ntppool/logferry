package main

import (
	"fmt"
	"sync"
	"testing"
)

/*
File Locking System Tests (filelock_test.go)

WHAT IS BEING TESTED:
- Constructor validation: NewFileLock() returns properly initialized instance
- Basic locking: Files can be locked successfully, including edge cases
- Double locking prevention: Same file cannot be locked twice
- Lock status checking: Lock state queries and isolation between filenames
- Unlocking: Unlocked files can be locked again
- Graceful error handling: Unlocking non-existent locks doesn't panic
- Concurrency safety: Only one goroutine can acquire the same lock
- Parallel locking: Different files can be locked simultaneously
- Lock lifecycle: Repeated lock/unlock cycles work correctly
- Empty string validation: Empty filenames are rejected (returns false)

WHAT IS NOT BEING TESTED:
- Memory cleanup after many lock/unlock cycles
- Behavior with extremely long filenames (>1000 chars)
- Performance under high contention
- Lock timeouts or expiration
- Persistence across process restarts
- Path traversal protection in filenames

NOTES:
- Empty string filenames are NOT allowed - Lock("") and IsLocked("") return false

QUESTIONS:
- What's the maximum supported filename length? Should there be validation or limits?
- Should there be any filename sanitization (e.g., path traversal protection)?
*/

// TestNewFileLock validates constructor creates properly initialized instance
func TestNewFileLock(t *testing.T) {
	fl := NewFileLock()
	if fl == nil {
		t.Fatal("NewFileLock() returned nil")
	}
	if fl.m == nil {
		t.Error("FileLock map is nil")
	}
}

// TestFileLock_Lock tests basic locking functionality including edge cases
func TestFileLock_Lock(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		want     bool
	}{
		{"lock new file", "test.avro", true},
		{"lock empty string", "", false},
		{"lock file with spaces", "test file.avro", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fl := NewFileLock()
			got := fl.Lock(tt.filename)
			if got != tt.want {
				t.Errorf("Lock() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestFileLock_DoubleLock ensures the same file cannot be locked twice (critical for preventing duplicate uploads)
func TestFileLock_DoubleLock(t *testing.T) {
	fl := NewFileLock()
	filename := "test.avro"

	// First lock should succeed
	if !fl.Lock(filename) {
		t.Error("First lock should succeed")
	}

	// Second lock should fail
	if fl.Lock(filename) {
		t.Error("Second lock should fail")
	}
}

func TestFileLock_IsLocked(t *testing.T) {
	fl := NewFileLock()
	filename := "test.avro"

	// File should not be locked initially
	if fl.IsLocked(filename) {
		t.Error("File should not be locked initially")
	}

	// Lock the file
	fl.Lock(filename)

	// File should now be locked
	if !fl.IsLocked(filename) {
		t.Error("File should be locked after Lock()")
	}

	// Different file should not be locked
	if fl.IsLocked("other.avro") {
		t.Error("Different file should not be locked")
	}
}

func TestFileLock_Unlock(t *testing.T) {
	fl := NewFileLock()
	filename := "test.avro"

	// Lock the file first
	fl.Lock(filename)
	if !fl.IsLocked(filename) {
		t.Error("File should be locked")
	}

	// Unlock the file
	fl.Unlock(filename)
	if fl.IsLocked(filename) {
		t.Error("File should be unlocked after Unlock()")
	}

	// Should be able to lock again after unlock
	if !fl.Lock(filename) {
		t.Error("Should be able to lock again after unlock")
	}
}

func TestFileLock_UnlockNonExistent(t *testing.T) {
	fl := NewFileLock()
	// Unlocking a file that was never locked should not panic
	fl.Unlock("nonexistent.avro")
	// Test passes if no panic occurs
}

// TestFileLock_ConcurrentAccess tests that only one goroutine can acquire the same lock (core concurrency safety)
func TestFileLock_ConcurrentAccess(t *testing.T) {
	fl := NewFileLock()
	filename := "concurrent.avro"
	var successCount int
	var mu sync.Mutex

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	// Launch multiple goroutines trying to lock the same file
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			if fl.Lock(filename) {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Only one goroutine should have successfully locked the file
	if successCount != 1 {
		t.Errorf("Expected exactly 1 successful lock, got %d", successCount)
	}

	// The file should still be locked
	if !fl.IsLocked(filename) {
		t.Error("File should still be locked after concurrent access")
	}
}

func TestFileLock_ConcurrentDifferentFiles(t *testing.T) {
	fl := NewFileLock()
	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	// Launch goroutines trying to lock different files
	for i := 0; i < goroutines; i++ {
		go func(fileNum int) {
			defer wg.Done()
			filename := fmt.Sprintf("file%d.avro", fileNum)
			if !fl.Lock(filename) {
				t.Errorf("Should be able to lock different file %s", filename)
			}
		}(i)
	}

	wg.Wait()

	// All files should be locked
	for i := 0; i < goroutines; i++ {
		filename := fmt.Sprintf("file%d.avro", i)
		if !fl.IsLocked(filename) {
			t.Errorf("File %s should be locked", filename)
		}
	}
}

func TestFileLock_LockUnlockCycle(t *testing.T) {
	fl := NewFileLock()
	filename := "cycle.avro"
	const cycles = 100

	for i := 0; i < cycles; i++ {
		if !fl.Lock(filename) {
			t.Fatalf("Lock failed on cycle %d", i)
		}
		if !fl.IsLocked(filename) {
			t.Fatalf("File not locked on cycle %d", i)
		}
		fl.Unlock(filename)
		if fl.IsLocked(filename) {
			t.Fatalf("File still locked after unlock on cycle %d", i)
		}
	}
}
