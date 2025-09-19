// bozobackup: Tests for streaming I/O optimization (Step 4.1)
package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestCopyFileWithHashAndTimestamps verifies the streaming I/O function works correctly
func TestCopyFileWithHashAndTimestamps(t *testing.T) {
	tempDir := t.TempDir()
	srcFile := filepath.Join(tempDir, "source.jpg")
	dstFile := filepath.Join(tempDir, "dest.jpg")

	// Create test file with known content
	testContent := []byte("test jpeg content for streaming I/O optimization")
	expectedHash := fmt.Sprintf("%x", sha256.Sum256(testContent))

	err := os.WriteFile(srcFile, testContent, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Set specific timestamps for testing
	testTime := time.Date(2023, 12, 25, 15, 30, 45, 0, time.UTC)
	err = os.Chtimes(srcFile, testTime, testTime)
	if err != nil {
		t.Fatalf("Failed to set test timestamps: %v", err)
	}

	ctx := context.Background()

	// Test the streaming copy function
	hash, err := copyFileWithHashAndTimestamps(ctx, srcFile, dstFile)
	if err != nil {
		t.Fatalf("copyFileWithHashAndTimestamps failed: %v", err)
	}

	// Verify hash is correct
	if hash != expectedHash {
		t.Errorf("Hash mismatch: expected %s, got %s", expectedHash, hash)
	}

	// Verify file was copied correctly
	copiedContent, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("Failed to read copied file: %v", err)
	}

	if string(copiedContent) != string(testContent) {
		t.Errorf("File content mismatch: expected %q, got %q", string(testContent), string(copiedContent))
	}

	// Verify timestamps were preserved
	srcInfo, err := os.Stat(srcFile)
	if err != nil {
		t.Fatalf("Failed to stat source file: %v", err)
	}

	dstInfo, err := os.Stat(dstFile)
	if err != nil {
		t.Fatalf("Failed to stat destination file: %v", err)
	}

	// Check modification time (allow 1 second tolerance for filesystem precision)
	srcMtime := srcInfo.ModTime()
	dstMtime := dstInfo.ModTime()
	timeDiff := srcMtime.Sub(dstMtime)
	if timeDiff < 0 {
		timeDiff = -timeDiff
	}
	if timeDiff > time.Second {
		t.Errorf("Timestamp not preserved: source %v, destination %v, diff %v", srcMtime, dstMtime, timeDiff)
	}
}

// TestCopyFileWithHashAndTimestampsCancellation verifies context cancellation works
func TestCopyFileWithHashAndTimestampsCancellation(t *testing.T) {
	tempDir := t.TempDir()
	srcFile := filepath.Join(tempDir, "source.jpg")
	dstFile := filepath.Join(tempDir, "dest.jpg")

	// Create test file
	testContent := []byte("test content for cancellation")
	err := os.WriteFile(srcFile, testContent, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Test with cancelled context
	_, err = copyFileWithHashAndTimestamps(ctx, srcFile, dstFile)
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled error, got: %v", err)
	}

	// Verify no destination file was created (cleanup worked)
	if _, err := os.Stat(dstFile); !os.IsNotExist(err) {
		t.Error("Destination file should not exist after cancellation")
	}

	// Verify no temp file was left behind
	tmpFile := dstFile + ".tmp"
	if _, err := os.Stat(tmpFile); !os.IsNotExist(err) {
		t.Error("Temp file should not exist after cancellation")
	}
}

// TestStreamingVsLegacyHashConsistency verifies streaming and legacy hash computation give same results
func TestStreamingVsLegacyHashConsistency(t *testing.T) {
	tempDir := t.TempDir()
	srcFile := filepath.Join(tempDir, "source.jpg")
	dstFile := filepath.Join(tempDir, "dest.jpg")

	// Create test file with various content types
	testCases := [][]byte{
		[]byte("small file"),
		make([]byte, 1024*1024), // 1MB file
		[]byte("file with special chars: áéíóú ñ 中文 🚀"),
	}

	for i, testContent := range testCases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			// Fill large test case with pattern
			if len(testContent) == 1024*1024 {
				for j := range testContent {
					testContent[j] = byte(j % 256)
				}
			}

			err := os.WriteFile(srcFile, testContent, 0644)
			if err != nil {
				t.Fatalf("Failed to create test file: %v", err)
			}

			// Compute hash using legacy method
			legacyHash := getFileHash(srcFile)
			if legacyHash == "" {
				t.Fatal("Legacy hash computation failed")
			}

			// Compute hash using streaming method
			ctx := context.Background()
			streamingHash, err := copyFileWithHashAndTimestamps(ctx, srcFile, dstFile)
			if err != nil {
				t.Fatalf("Streaming hash computation failed: %v", err)
			}

			// Verify hashes match
			if legacyHash != streamingHash {
				t.Errorf("Hash mismatch: legacy %s, streaming %s", legacyHash, streamingHash)
			}

			// Clean up for next iteration
			os.Remove(dstFile)
		})
	}
}

// BenchmarkStreamingVsLegacyIO benchmarks streaming I/O vs separate hash+copy operations
func BenchmarkStreamingVsLegacyIO(b *testing.B) {
	tempDir := b.TempDir()
	srcFile := filepath.Join(tempDir, "source.dat")

	// Create 10MB test file for meaningful benchmark
	testContent := make([]byte, 10*1024*1024)
	for i := range testContent {
		testContent[i] = byte(i % 256)
	}

	err := os.WriteFile(srcFile, testContent, 0644)
	if err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	ctx := context.Background()

	b.Run("Legacy_Hash_Plus_Copy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dstFile := filepath.Join(tempDir, fmt.Sprintf("legacy_dest_%d.dat", i))

			// Separate hash computation and copy (current approach)
			hash := getFileHash(srcFile)
			if hash == "" {
				b.Fatal("Hash computation failed")
			}

			err := copyFileWithTimestamps(ctx, srcFile, dstFile)
			if err != nil {
				b.Fatalf("File copy failed: %v", err)
			}

			// Clean up
			os.Remove(dstFile)
		}
	})

	b.Run("Streaming_Hash_And_Copy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dstFile := filepath.Join(tempDir, fmt.Sprintf("streaming_dest_%d.dat", i))

			// Streaming hash computation and copy (optimized approach)
			_, err := copyFileWithHashAndTimestamps(ctx, srcFile, dstFile)
			if err != nil {
				b.Fatalf("Streaming copy failed: %v", err)
			}

			// Clean up
			os.Remove(dstFile)
		}
	})
}

// TestClassifyAndProcessFileStreaming verifies the streaming classification works correctly
func TestClassifyAndProcessFileStreaming(t *testing.T) {
	tempDir := t.TempDir()
	srcDir := filepath.Join(tempDir, "src")
	destDir := filepath.Join(tempDir, "dest")
	dbPath := filepath.Join(tempDir, "test.db")

	os.MkdirAll(srcDir, 0755)
	os.MkdirAll(destDir, 0755)

	db := initDB(dbPath)
	defer db.Close()

	ctx := context.Background()

	// Test file that should be copied
	testFile := filepath.Join(srcDir, "test.jpg")
	testContent := []byte("test jpeg for streaming classification")
	expectedHash := fmt.Sprintf("%x", sha256.Sum256(testContent))

	err := os.WriteFile(testFile, testContent, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create FileCandidate
	candidate, err := NewFileCandidate(testFile, destDir)
	if err != nil {
		t.Fatalf("Failed to create FileCandidate: %v", err)
	}

	// Create hash cache for testing
	hashCache, err := NewHashCache(db)
	if err != nil {
		t.Fatalf("Failed to create hash cache: %v", err)
	}

	// Test streaming classification and processing
	result := classifyAndProcessFile(ctx, candidate, hashCache, false, 0)

	// Verify file was copied
	if result.FinalState != StateCopied {
		t.Errorf("Expected StateCopied, got %v", result.FinalState)
	}

	// Verify hash was computed correctly during streaming
	if candidate.Hash != expectedHash {
		t.Errorf("Hash mismatch: expected %s, got %s", expectedHash, candidate.Hash)
	}

	// Verify file was actually copied to destination
	if _, err := os.Stat(candidate.DestPath); os.IsNotExist(err) {
		t.Error("Destination file should exist after streaming copy")
	}

	// Verify database record was inserted
	if !result.DBInserted {
		t.Error("Database record should have been inserted")
	}

	// Verify bytes copied is correct
	if result.BytesCopied != int64(len(testContent)) {
		t.Errorf("Expected %d bytes copied, got %d", len(testContent), result.BytesCopied)
	}
}