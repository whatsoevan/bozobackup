// bozobackup: Tests for database optimization (Step 4.2)
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

// TestHashCachePreloading verifies hash cache pre-loading works correctly
func TestHashCachePreloading(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Initialize database
	db := initDB(dbPath)
	defer db.Close()

	// Insert some test records directly
	testHashes := []string{
		"hash1234567890abcdef",
		"hash2468101214161820",
		"hash3691215182124273",
	}

	for i, hash := range testHashes {
		_, err := db.Exec("INSERT INTO files (src_path, dest_path, hash, size, mtime, copied_at) VALUES (?, ?, ?, ?, ?, ?)",
			fmt.Sprintf("/test/file%d.jpg", i),
			fmt.Sprintf("/dest/file%d.jpg", i),
			hash,
			1000+i,
			time.Now().Unix(),
			time.Now().Format(time.RFC3339))
		if err != nil {
			t.Fatalf("Failed to insert test record: %v", err)
		}
	}

	// Create hash cache and verify pre-loading
	hashCache, err := NewHashCache(db)
	if err != nil {
		t.Fatalf("Failed to create hash cache: %v", err)
	}

	// Verify all hashes were pre-loaded
	for _, hash := range testHashes {
		if !hashCache.IsProcessed(hash) {
			t.Errorf("Hash %s should be in pre-loaded cache", hash)
		}
	}

	// Verify non-existent hash is not found
	if hashCache.IsProcessed("nonexistent_hash") {
		t.Error("Non-existent hash should not be in cache")
	}
}

// TestHashCacheBatchOperations verifies batch database operations work correctly
func TestHashCacheBatchOperations(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Initialize database and hash cache
	db := initDB(dbPath)
	defer db.Close()

	hashCache, err := NewHashCache(db)
	if err != nil {
		t.Fatalf("Failed to create hash cache: %v", err)
	}

	// Add records to batch
	testFiles := []struct {
		path string
		hash string
		size int64
	}{
		{"/test/file1.jpg", "batch_hash_1", 1001},
		{"/test/file2.jpg", "batch_hash_2", 1002},
		{"/test/file3.jpg", "batch_hash_3", 1003},
	}

	for _, file := range testFiles {
		hashCache.AddToBatch(file.path, "/dest"+file.path, file.hash, file.size, time.Now().Unix())
	}

	// Verify hashes are immediately available in memory cache
	for _, file := range testFiles {
		if !hashCache.IsProcessed(file.hash) {
			t.Errorf("Hash %s should be immediately available in memory cache", file.hash)
		}
	}

	// Flush batch to database
	err = hashCache.FlushBatch()
	if err != nil {
		t.Fatalf("Failed to flush batch: %v", err)
	}

	// Verify records were actually inserted into database
	for _, file := range testFiles {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM files WHERE hash = ?", file.hash).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query database: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 record for hash %s, got %d", file.hash, count)
		}
	}
}

// TestOptimizedEvaluationFunction verifies the optimized evaluation function works correctly
func TestOptimizedEvaluationFunction(t *testing.T) {
	tempDir := t.TempDir()
	srcDir := filepath.Join(tempDir, "src")
	destDir := filepath.Join(tempDir, "dest")
	dbPath := filepath.Join(tempDir, "test.db")

	os.MkdirAll(srcDir, 0755)
	os.MkdirAll(destDir, 0755)

	// Initialize database and hash cache
	db := initDB(dbPath)
	defer db.Close()

	hashCache, err := NewHashCache(db)
	if err != nil {
		t.Fatalf("Failed to create hash cache: %v", err)
	}

	// Test file that should be copied
	testFile := filepath.Join(srcDir, "test.jpg")
	testContent := []byte("test content for database optimization")
	expectedHash := fmt.Sprintf("%x", sha256.Sum256(testContent))

	err = os.WriteFile(testFile, testContent, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create FileCandidate
	candidate, err := NewFileCandidate(testFile, destDir)
	if err != nil {
		t.Fatalf("Failed to create FileCandidate: %v", err)
	}

	// Test evaluation function
	decision := evaluateFileForBackup(candidate, hashCache, false, 0)

	// Verify file should be copied (not a duplicate)
	if decision.State != StateCopied {
		t.Errorf("Expected StateCopied, got %v", decision.State)
	}
	if !decision.ShouldCopy {
		t.Error("Expected ShouldCopy to be true")
	}

	// Verify hash was computed correctly
	if candidate.Hash != expectedHash {
		t.Errorf("Hash mismatch: expected %s, got %s", expectedHash, candidate.Hash)
	}

	// Add hash to cache and test duplicate detection
	hashCache.AddToBatch(testFile, candidate.DestPath, candidate.Hash, candidate.Info.Size(), candidate.Info.ModTime().Unix())

	// Create second file with same content
	duplicateFile := filepath.Join(srcDir, "duplicate.jpg")
	err = os.WriteFile(duplicateFile, testContent, 0644)
	if err != nil {
		t.Fatalf("Failed to create duplicate file: %v", err)
	}

	duplicateCandidate, err := NewFileCandidate(duplicateFile, destDir)
	if err != nil {
		t.Fatalf("Failed to create duplicate FileCandidate: %v", err)
	}

	// Test duplicate detection
	duplicateDecision := evaluateFileForBackup(duplicateCandidate, hashCache, false, 0)

	if duplicateDecision.State != StateDuplicateHash {
		t.Errorf("Expected StateDuplicateHash, got %v", duplicateDecision.State)
	}
	if duplicateDecision.ShouldCopy {
		t.Error("Expected ShouldCopy to be false for duplicate")
	}
}

// TestEndToEndOptimizedProcessing verifies the complete optimized processing pipeline
func TestEndToEndOptimizedProcessing(t *testing.T) {
	tempDir := t.TempDir()
	srcDir := filepath.Join(tempDir, "src")
	destDir := filepath.Join(tempDir, "dest")
	dbPath := filepath.Join(tempDir, "test.db")

	os.MkdirAll(srcDir, 0755)
	os.MkdirAll(destDir, 0755)

	// Initialize database and hash cache
	db := initDB(dbPath)
	defer db.Close()

	hashCache, err := NewHashCache(db)
	if err != nil {
		t.Fatalf("Failed to create hash cache: %v", err)
	}

	ctx := context.Background()

	// Create test files
	testFiles := map[string][]byte{
		"photo1.jpg": []byte("photo 1 content"),
		"photo2.jpg": []byte("photo 2 content"),
		"duplicate.jpg": []byte("photo 1 content"), // Same as photo1
	}

	for filename, content := range testFiles {
		filePath := filepath.Join(srcDir, filename)
		err := os.WriteFile(filePath, content, 0644)
		if err != nil {
			t.Fatalf("Failed to create file %s: %v", filename, err)
		}
	}

	var results []*ProcessingResult

	// Process all files using optimized pipeline
	for filename := range testFiles {
		filePath := filepath.Join(srcDir, filename)
		candidate, err := NewFileCandidate(filePath, destDir)
		if err != nil {
			t.Fatalf("Failed to create candidate for %s: %v", filename, err)
		}

		result := classifyAndProcessFile(ctx, candidate, hashCache, false, 0)
		results = append(results, result)
	}

	// Flush any remaining batched records
	err = hashCache.FlushBatch()
	if err != nil {
		t.Fatalf("Failed to flush final batch: %v", err)
	}

	// Verify results
	copiedCount := 0
	duplicateCount := 0

	for _, result := range results {
		switch result.FinalState {
		case StateCopied:
			copiedCount++
			// Verify file was actually copied
			if _, err := os.Stat(result.Candidate.DestPath); os.IsNotExist(err) {
				t.Errorf("File %s should have been copied to %s", result.Candidate.Path, result.Candidate.DestPath)
			}
		case StateDuplicateHash:
			duplicateCount++
		}
	}

	// We should have 2 files copied and 1 duplicate detected
	if copiedCount != 2 {
		t.Errorf("Expected 2 files copied, got %d", copiedCount)
	}
	if duplicateCount != 1 {
		t.Errorf("Expected 1 duplicate detected, got %d", duplicateCount)
	}

	// Verify database records
	var dbRecordCount int
	err = db.QueryRow("SELECT COUNT(*) FROM files").Scan(&dbRecordCount)
	if err != nil {
		t.Fatalf("Failed to count database records: %v", err)
	}

	if dbRecordCount != 2 {
		t.Errorf("Expected 2 database records, got %d", dbRecordCount)
	}
}