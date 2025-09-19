// bozobackup: Incremental, deduplicating photo/video backup tool with HTML reporting.
package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "modernc.org/sqlite"
)

func initDB(dbPath string) *sql.DB {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[FATAL] Could not open database: %v\n", err)
		os.Exit(1)
	}
	sqlStmt := `
	CREATE TABLE IF NOT EXISTS files (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		src_path TEXT,
		dest_path TEXT,
		hash TEXT UNIQUE,
		size INTEGER,
		mtime INTEGER,
		copied_at TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_hash ON files(hash);
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[FATAL] Could not initialize database schema: %v\n", err)
		db.Close()
		os.Exit(1)
	}
	return db
}

// HashCache provides fast in-memory duplicate checking with batch database operations
type HashCache struct {
	hashes map[string]bool  // Pre-loaded hash set for O(1) duplicate checking
	batch  []FileRecord     // Batch of records to insert
	db     *sql.DB
}

// FileRecord represents a file record for batch insertion
type FileRecord struct {
	SrcPath  string
	DestPath string
	Hash     string
	Size     int64
	Mtime    int64
}

// NewHashCache creates and pre-loads a hash cache from the database
func NewHashCache(db *sql.DB) (*HashCache, error) {
	cache := &HashCache{
		hashes: make(map[string]bool),
		batch:  make([]FileRecord, 0),
		db:     db,
	}

	// Pre-load all existing hashes into memory for O(1) duplicate checking
	rows, err := db.Query("SELECT hash FROM files")
	if err != nil {
		return nil, fmt.Errorf("failed to pre-load hashes: %w", err)
	}
	defer rows.Close()

	hashCount := 0
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return nil, fmt.Errorf("failed to scan hash: %w", err)
		}
		cache.hashes[hash] = true
		hashCount++
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating hashes: %w", err)
	}

	log.Printf("Pre-loaded %d hashes into memory cache", hashCount)
	return cache, nil
}

// IsProcessed checks if a hash exists using O(1) memory lookup (no database query)
func (hc *HashCache) IsProcessed(hash string) bool {
	return hc.hashes[hash]
}

// AddToBatch adds a file record to the batch for later insertion
func (hc *HashCache) AddToBatch(srcPath, destPath, hash string, size, mtime int64) {
	// Add to memory cache immediately so subsequent lookups find it
	hc.hashes[hash] = true

	// Add to batch for database insertion
	hc.batch = append(hc.batch, FileRecord{
		SrcPath:  srcPath,
		DestPath: destPath,
		Hash:     hash,
		Size:     size,
		Mtime:    mtime,
	})
}

// FlushBatch inserts all batched records to database and clears the batch
func (hc *HashCache) FlushBatch() error {
	if len(hc.batch) == 0 {
		return nil
	}

	// Prepare batch insert statement
	tx, err := hc.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT OR IGNORE INTO files (src_path, dest_path, hash, size, mtime, copied_at) VALUES (?, ?, ?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Insert all records in batch
	currentTime := time.Now().Format(time.RFC3339)
	for _, record := range hc.batch {
		_, err := stmt.Exec(record.SrcPath, record.DestPath, record.Hash, record.Size, record.Mtime, currentTime)
		if err != nil {
			return fmt.Errorf("failed to insert record: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("Flushed %d records to database", len(hc.batch))
	hc.batch = hc.batch[:0] // Clear batch
	return nil
}


// getLastBackupTime returns the most recent copied_at time from the DB, or zero if none
func getLastBackupTime(db *sql.DB) (time.Time, error) {
	row := db.QueryRow("SELECT MAX(copied_at) FROM files WHERE copied_at IS NOT NULL")
	var last string
	err := row.Scan(&last)
	if err != nil || last == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339, last)
	if err != nil {
		return time.Time{}, nil
	}
	return parsed, nil
}
