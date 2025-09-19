// bozobackup: Tests for unified file classification system (Step 2.1)
package main

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Test that classifyAndProcessFile provides consistent, definitive file states
func TestClassifyAndProcessFileConsistency(t *testing.T) {
	// Create temp directory and database for testing
	tempDir := t.TempDir()
	srcDir := filepath.Join(tempDir, "src")
	destDir := filepath.Join(tempDir, "dest")
	dbPath := filepath.Join(tempDir, "test.db")

	os.MkdirAll(srcDir, 0755)
	os.MkdirAll(destDir, 0755)

	db := initDB(dbPath)
	defer db.Close()

	ctx := context.Background()

	tests := []struct {
		name           string
		setup          func() string                      // Returns test file path
		incremental    bool
		minMtime       int64
		expectedState  FileState
		shouldCopy     bool
		shouldInsertDB bool
	}{
		{
			name: "Valid JPEG should be copied",
			setup: func() string {
				filePath := filepath.Join(srcDir, "test.jpg")
				content := []byte("fake jpeg content")
				os.WriteFile(filePath, content, 0644)
				return filePath
			},
			incremental:    false,
			expectedState:  StateCopied,
			shouldCopy:     true,
			shouldInsertDB: true,
		},
		{
			name: "Invalid extension should be skipped",
			setup: func() string {
				filePath := filepath.Join(srcDir, "test.txt")
				content := []byte("text file content")
				os.WriteFile(filePath, content, 0644)
				return filePath
			},
			incremental:   false,
			expectedState: StateSkippedExtension,
			shouldCopy:    false,
		},
		{
			name: "File older than last backup should be skipped in incremental mode",
			setup: func() string {
				filePath := filepath.Join(srcDir, "old.jpg")
				content := []byte("old jpeg content")
				os.WriteFile(filePath, content, 0644)

				// Set modification time to past
				pastTime := time.Now().Add(-2 * time.Hour)
				os.Chtimes(filePath, pastTime, pastTime)
				return filePath
			},
			incremental:   true,
			minMtime:      time.Now().Add(-1 * time.Hour).Unix(),
			expectedState: StateSkippedIncremental,
			shouldCopy:    false,
		},
		{
			name: "Duplicate hash should be detected",
			setup: func() string {
				// First, create and "process" a file to get its hash in the database
				firstFile := filepath.Join(srcDir, "original.jpg")
				content := []byte("duplicate content")
				os.WriteFile(firstFile, content, 0644)

				candidate, _ := NewFileCandidate(firstFile, destDir)
				candidate.EnsureHash()

				// Insert hash into database manually using direct SQL
				_, insertErr := db.Exec("INSERT INTO files (src_path, dest_path, hash, size, mtime, copied_at) VALUES (?, ?, ?, ?, ?, ?)",
					firstFile, "/fake/dest/path", candidate.Hash, int64(len(content)), time.Now().Unix(), time.Now().Format(time.RFC3339))
				if insertErr != nil {
					t.Fatalf("Failed to insert test record: %v", insertErr)
				}

				// Now create second file with same content
				duplicateFile := filepath.Join(srcDir, "duplicate.jpg")
				os.WriteFile(duplicateFile, content, 0644)
				return duplicateFile
			},
			incremental:   false,
			expectedState: StateDuplicateHash,
			shouldCopy:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filePath := tt.setup()

			// Create FileCandidate
			candidate, err := NewFileCandidate(filePath, destDir)
			if err != nil {
				t.Fatalf("Failed to create FileCandidate: %v", err)
			}

			// Create hash cache for testing
			hashCache, err := NewHashCache(db)
			if err != nil {
				t.Fatalf("Failed to create hash cache: %v", err)
			}

			// Test unified classification and processing
			result := classifyAndProcessFile(ctx, candidate, hashCache, tt.incremental, tt.minMtime)

			// Verify final state is consistent
			if result.FinalState != tt.expectedState {
				t.Errorf("Expected final state %v, got %v", tt.expectedState, result.FinalState)
			}

			// Verify decision consistency
			if result.Decision.ShouldCopy != tt.shouldCopy {
				t.Errorf("Expected ShouldCopy %v, got %v", tt.shouldCopy, result.Decision.ShouldCopy)
			}

			// For files that should be copied, verify they actually were
			if tt.shouldCopy && result.FinalState == StateCopied {
				if result.BytesCopied == 0 {
					t.Error("File marked as copied but BytesCopied is 0")
				}
				if !result.DBInserted {
					t.Error("File marked as copied but not inserted in database")
				}
				// Verify destination file exists
				if _, err := os.Stat(candidate.DestPath); os.IsNotExist(err) {
					t.Error("File marked as copied but destination file doesn't exist")
				}
			}

			// For files that shouldn't be copied, verify they weren't
			if !tt.shouldCopy {
				if result.BytesCopied != 0 {
					t.Error("File marked as not copied but BytesCopied > 0")
				}
				if result.DBInserted {
					t.Error("File marked as not copied but inserted in database")
				}
			}

			// Verify consistent classification (no flip-flopping between states)
			if result.Decision.State != result.FinalState && result.Decision.ShouldCopy {
				// Only allowed change is StateCopied -> StateCopied (success) or StateErrorCopy (failure)
				if result.Decision.State == StateCopied && result.FinalState != StateCopied && result.FinalState != StateErrorCopy {
					t.Errorf("Inconsistent classification: decision said %v but final state is %v",
						result.Decision.State, result.FinalState)
				}
			}
		})
	}
}

// Test that AccountingSummary provides perfect accounting
func TestAccountingSummaryPerfectAccounting(t *testing.T) {
	// Create mock ProcessingResults with different states
	results := []*ProcessingResult{
		{
			Candidate:  &FileCandidate{Path: "/test/copied.jpg", DestPath: "/dest/copied.jpg"},
			FinalState: StateCopied,
			BytesCopied: 1000,
		},
		{
			Candidate:  &FileCandidate{Path: "/test/skipped.txt"},
			FinalState: StateSkippedExtension,
		},
		{
			Candidate:  &FileCandidate{Path: "/test/duplicate.jpg", DestPath: "/dest/duplicate.jpg"},
			FinalState: StateDuplicateHash,
		},
		{
			Candidate:  &FileCandidate{Path: "/test/error.jpg"},
			FinalState: StateErrorHash,
			Error:      sql.ErrNoRows,
		},
	}

	walkErrors := []error{
		os.ErrPermission,
	}

	// Generate accounting summary
	summary := GenerateAccountingSummary(results, walkErrors)

	// Verify perfect accounting
	if err := summary.Validate(); err != nil {
		t.Errorf("Accounting validation failed: %v", err)
	}

	// Verify counts
	expectedCopied := 1
	expectedSkipped := 1
	expectedDuplicates := 1
	expectedErrors := 2 // 1 processing error + 1 walk error

	if summary.Copied != expectedCopied {
		t.Errorf("Expected %d copied, got %d", expectedCopied, summary.Copied)
	}
	if summary.Skipped != expectedSkipped {
		t.Errorf("Expected %d skipped, got %d", expectedSkipped, summary.Skipped)
	}
	if summary.Duplicates != expectedDuplicates {
		t.Errorf("Expected %d duplicates, got %d", expectedDuplicates, summary.Duplicates)
	}
	if summary.Errors != expectedErrors {
		t.Errorf("Expected %d errors, got %d", expectedErrors, summary.Errors)
	}

	// Verify total bytes
	if summary.TotalBytes != 1000 {
		t.Errorf("Expected 1000 total bytes, got %d", summary.TotalBytes)
	}

	// Verify file lists are populated correctly
	if len(summary.CopiedFiles) != 1 {
		t.Errorf("Expected 1 copied file entry, got %d", len(summary.CopiedFiles))
	}
	if len(summary.SkippedFiles) != 1 {
		t.Errorf("Expected 1 skipped file entry, got %d", len(summary.SkippedFiles))
	}
	if len(summary.DuplicateFiles) != 1 {
		t.Errorf("Expected 1 duplicate file entry, got %d", len(summary.DuplicateFiles))
	}
	if len(summary.ErrorList) != 2 {
		t.Errorf("Expected 2 error entries, got %d", len(summary.ErrorList))
	}
}

// Test FileState enum consistency
func TestFileStateStringRepresentation(t *testing.T) {
	tests := []struct {
		state    FileState
		expected string
	}{
		{StateCopied, "copied"},
		{StateSkippedExtension, "skipped (extension)"},
		{StateSkippedIncremental, "skipped (incremental)"},
		{StateSkippedDate, "skipped (no date)"},
		{StateSkippedDestExists, "skipped (destination exists)"},
		{StateDuplicateHash, "duplicate (hash exists)"},
		{StateErrorStat, "error (stat)"},
		{StateErrorDate, "error (date extraction)"},
		{StateErrorHash, "error (hash computation)"},
		{StateErrorCopy, "error (copy failed)"},
		{StateErrorWalk, "error (walk failed)"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if tt.state.String() != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, tt.state.String())
			}
		})
	}
}

// Test ProcessingResult helper methods
func TestProcessingResultHelpers(t *testing.T) {
	tests := []struct {
		name          string
		state         FileState
		expectSuccess bool
		expectError   bool
	}{
		{"Copied file is success", StateCopied, true, false},
		{"Skipped file is success", StateSkippedExtension, true, false},
		{"Duplicate is success", StateDuplicateHash, true, false},
		{"Error is not success", StateErrorCopy, false, true},
		{"Stat error is error", StateErrorStat, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &ProcessingResult{FinalState: tt.state}

			if result.IsSuccess() != tt.expectSuccess {
				t.Errorf("Expected IsSuccess() = %v, got %v", tt.expectSuccess, result.IsSuccess())
			}
			if result.IsError() != tt.expectError {
				t.Errorf("Expected IsError() = %v, got %v", tt.expectError, result.IsError())
			}
		})
	}
}