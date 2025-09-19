// bozobackup: Performance benchmarks for database optimization (Step 4.2)
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

// BenchmarkHashLookupPerformance compares O(N) database queries vs O(1) memory lookups
func BenchmarkHashLookupPerformance(b *testing.B) {
	tempDir := b.TempDir()
	dbPath := filepath.Join(tempDir, "benchmark.db")

	// Initialize database
	db := initDB(dbPath)
	defer db.Close()

	// Insert 100 test records for fast benchmarking
	testHashes := make([]string, 100)
	for i := 0; i < 100; i++ {
		content := fmt.Sprintf("test content %d", i)
		hash := fmt.Sprintf("%x", sha256.Sum256([]byte(content)))
		testHashes[i] = hash

		_, err := db.Exec("INSERT INTO files (src_path, dest_path, hash, size, mtime, copied_at) VALUES (?, ?, ?, ?, ?, ?)",
			fmt.Sprintf("/test/file%d.jpg", i),
			fmt.Sprintf("/dest/file%d.jpg", i),
			hash,
			1000+i,
			time.Now().Unix(),
			time.Now().Format(time.RFC3339))
		if err != nil {
			b.Fatalf("Failed to insert test record: %v", err)
		}
	}

	// Create hash cache for optimized approach
	hashCache, err := NewHashCache(db)
	if err != nil {
		b.Fatalf("Failed to create hash cache: %v", err)
	}

	// Test hashes that exist and don't exist
	existingHashes := testHashes[:50]           // First 50 hashes exist
	nonExistentHashes := make([]string, 50)    // 50 hashes that don't exist
	for i := 0; i < 50; i++ {
		nonExistentHashes[i] = fmt.Sprintf("nonexistent_hash_%d", i)
	}

	b.Run("OptimizedMemoryLookup", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, hash := range existingHashes {
				_ = hashCache.IsProcessed(hash) // O(1) memory lookup
			}
			for _, hash := range nonExistentHashes {
				_ = hashCache.IsProcessed(hash) // O(1) memory lookup
			}
		}
	})
}

// BenchmarkBatchVsIndividualInserts compares batch database operations vs individual inserts
func BenchmarkBatchVsIndividualInserts(b *testing.B) {
	tempDir := b.TempDir()

	b.Run("IndividualInserts", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			dbPath := filepath.Join(tempDir, fmt.Sprintf("individual_%d.db", i))
			db := initDB(dbPath)
			b.StartTimer()

			// Simulate 10 individual database inserts
			for j := 0; j < 10; j++ {
				_, err := db.Exec("INSERT INTO files (src_path, dest_path, hash, size, mtime, copied_at) VALUES (?, ?, ?, ?, ?, ?)",
					fmt.Sprintf("/test/file%d.jpg", j),
					fmt.Sprintf("/dest/file%d.jpg", j),
					fmt.Sprintf("hash_%d_%d", i, j),
					1000+j,
					time.Now().Unix(),
					time.Now().Format(time.RFC3339))
				if err != nil {
					b.Fatalf("Failed to insert record: %v", err)
				}
			}

			b.StopTimer()
			db.Close()
			b.StartTimer()
		}
	})

	b.Run("BatchInserts", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			dbPath := filepath.Join(tempDir, fmt.Sprintf("batch_%d.db", i))
			db := initDB(dbPath)
			hashCache, err := NewHashCache(db)
			if err != nil {
				b.Fatalf("Failed to create hash cache: %v", err)
			}
			b.StartTimer()

			// Simulate 10 files added to batch
			for j := 0; j < 10; j++ {
				hashCache.AddToBatch(
					fmt.Sprintf("/test/file%d.jpg", j),
					fmt.Sprintf("/dest/file%d.jpg", j),
					fmt.Sprintf("hash_%d_%d", i, j),
					int64(1000+j),
					time.Now().Unix())
			}

			// Single batch flush
			err = hashCache.FlushBatch()
			if err != nil {
				b.Fatalf("Failed to flush batch: %v", err)
			}

			b.StopTimer()
			db.Close()
			b.StartTimer()
		}
	})
}

// BenchmarkCompleteOptimizedWorkflow benchmarks the entire optimized backup workflow
func BenchmarkCompleteOptimizedWorkflow(b *testing.B) {
	tempDir := b.TempDir()
	srcDir := filepath.Join(tempDir, "src")
	destDir := filepath.Join(tempDir, "dest")

	os.MkdirAll(srcDir, 0755)
	os.MkdirAll(destDir, 0755)

	// Create test files
	testFiles := make([]string, 5) // 5 files for fast benchmark
	for i := 0; i < 5; i++ {
		filename := fmt.Sprintf("photo%d.jpg", i)
		filePath := filepath.Join(srcDir, filename)
		content := []byte(fmt.Sprintf("photo content %d", i))

		err := os.WriteFile(filePath, content, 0644)
		if err != nil {
			b.Fatalf("Failed to create test file: %v", err)
		}
		testFiles[i] = filePath
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dbPath := filepath.Join(tempDir, fmt.Sprintf("workflow_%d.db", i))
		db := initDB(dbPath)
		hashCache, err := NewHashCache(db)
		if err != nil {
			b.Fatalf("Failed to create hash cache: %v", err)
		}
		ctx := context.Background()
		b.StartTimer()

		// Process all files using optimized workflow
		for _, filePath := range testFiles {
			candidate, err := NewFileCandidate(filePath, destDir)
			if err != nil {
				b.Fatalf("Failed to create candidate: %v", err)
			}

			result := classifyAndProcessFile(ctx, candidate, hashCache, false, 0)
			if result.FinalState == StateErrorCopy || result.FinalState == StateErrorHash {
				b.Fatalf("Processing failed: %v", result.Error)
			}
		}

		// Flush batch
		err = hashCache.FlushBatch()
		if err != nil {
			b.Fatalf("Failed to flush batch: %v", err)
		}

		b.StopTimer()
		db.Close()
		// Clean up destination files for next iteration
		os.RemoveAll(destDir)
		os.MkdirAll(destDir, 0755)
		b.StartTimer()
	}
}

// BenchmarkHashCachePreloading measures the time to pre-load hashes from database
func BenchmarkHashCachePreloading(b *testing.B) {
	tempDir := b.TempDir()

	// Create databases with different numbers of records
	testSizes := []int{100, 1000}

	for _, size := range testSizes {
		dbPath := filepath.Join(tempDir, fmt.Sprintf("preload_%d.db", size))
		db := initDB(dbPath)

		// Insert test records
		for i := 0; i < size; i++ {
			hash := fmt.Sprintf("hash_%d", i)
			_, err := db.Exec("INSERT INTO files (src_path, dest_path, hash, size, mtime, copied_at) VALUES (?, ?, ?, ?, ?, ?)",
				fmt.Sprintf("/test/file%d.jpg", i),
				fmt.Sprintf("/dest/file%d.jpg", i),
				hash,
				1000+i,
				time.Now().Unix(),
				time.Now().Format(time.RFC3339))
			if err != nil {
				b.Fatalf("Failed to insert test record: %v", err)
			}
		}
		db.Close()

		b.Run(fmt.Sprintf("Preload_%d_Records", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				db := initDB(dbPath)
				_, err := NewHashCache(db)
				if err != nil {
					b.Fatalf("Failed to create hash cache: %v", err)
				}
				db.Close()
			}
		})
	}
}