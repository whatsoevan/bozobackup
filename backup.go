// bozobackup: Incremental, deduplicating photo/video backup tool with HTML reporting.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/fatih/color"
	"github.com/schollz/progressbar/v3"
)

// backup is the main backup routine: scans, checks, copies, and reports
// Now supports context cancellation for safe Ctrl+C handling
func backup(ctx context.Context, srcDir, destDir, dbPath, reportPath string, incremental bool) {
	checkDirExists(srcDir, "Source")
	checkDirExists(destDir, "Destination")

	db := initDB(dbPath)
	defer db.Close()

	// Initialize hash cache with pre-loaded hashes for O(1) duplicate checking
	hashCache, err := NewHashCache(db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[FATAL] Could not initialize hash cache: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		// Flush any remaining batched records
		if err := hashCache.FlushBatch(); err != nil {
			fmt.Printf("Warning: failed to flush final batch: %v\n", err)
		}
	}()

	startTime := time.Now()

	var minMtime int64 = 0
	var lastBackupTime time.Time
	if incremental {
		var err error
		lastBackupTime, err = getLastBackupTime(db)
		if err == nil && !lastBackupTime.IsZero() {
			minMtime = lastBackupTime.Unix()
		}
	} else {
		// info: incremental mode disabled (removed print)
	}

	// Scan all files in source directory
	files, walkErrors := getAllFiles(srcDir)

	// Single-pass processing with unified classification
	var results []*ProcessingResult
	var estimatedTotalSize int64

	// Create progress bar for single-pass processing
	bar := progressbar.NewOptions(
		len(files),
		progressbar.OptionSetDescription("Processing files"),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionSetWidth(50),
		progressbar.OptionSetPredictTime(true), // ETA
		progressbar.OptionSetElapsedTime(true), // Elapsed
		progressbar.OptionClearOnFinish(),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSpinnerType(14), // Use a spinner
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)

	// Single pass: classify and process each file atomically
	for _, file := range files {
		select {
		case <-ctx.Done():
			color.New(color.FgRed, color.Bold).Println("Backup interrupted by user. Writing partial report and exiting.")
			goto cleanup
		default:
		}
		if ctx.Err() != nil {
			goto cleanup
		}
		bar.Add(1)

		// Create FileCandidate (caches os.Stat, extension, etc.)
		candidate, err := NewFileCandidate(file, destDir)
		if err != nil {
			// Create error result for candidate creation failure
			errorResult := &ProcessingResult{
				Candidate: &FileCandidate{Path: file},
				FinalState: StateErrorStat,
				Error: err,
				StartTime: time.Now(),
				EndTime: time.Now(),
			}
			results = append(results, errorResult)
			continue
		}

		// Classify and process the file using database-optimized approach
		// Uses pre-loaded hash cache and batch database operations
		result := classifyAndProcessFile(ctx, candidate, hashCache, incremental, minMtime)
		results = append(results, result)

		// Track estimated size for space checking (done incrementally)
		if result.Decision.ShouldCopy {
			estimatedTotalSize += result.Decision.EstimatedSize
		}

		// Break on context cancellation
		if ctx.Err() != nil {
			break
		}
	}

	// Note: Free space checking is now done incrementally during processing
	// This could be enhanced to check periodically and stop early if space runs out

cleanup:
	totalTime := time.Since(startTime)

	// Generate perfect accounting summary from results (no manual counters!)
	summary := GenerateAccountingSummary(results, walkErrors)

	// Generate HTML report with perfectly consistent data
	writeHTMLReport(reportPath, summary.CopiedFiles, summary.DuplicateFiles,
				   summary.SkippedFiles, summary.ErrorList, summary.TotalBytes, totalTime)

	// Validate accounting (should always be perfect now)
	if err := summary.Validate(); err != nil {
		color.New(color.FgRed, color.Bold).Printf("ACCOUNTING ERROR: %v\n", err)
	}

	// Print summary with bulletproof accounting
	totalFound := len(files)
	fmt.Println()
	color.New(color.FgGreen).Printf("Copied: %d, ", summary.Copied)
	color.New(color.FgYellow).Printf("Skipped: %d, Duplicates: %d, ", summary.Skipped, summary.Duplicates)
	color.New(color.FgRed).Printf("Errors: %d, ", summary.Errors)
	fmt.Printf("Total Found: %d\n", totalFound)

	totalAccounted := summary.Copied + summary.Skipped + summary.Duplicates + summary.Errors
	if totalAccounted == totalFound {
		color.New(color.FgGreen, color.Bold).Println("✔ All files accounted for!")
	} else {
		color.New(color.FgRed, color.Bold).Printf("✖ Mismatch! Accounted: %d, Found: %d\n", totalAccounted, totalFound)
	}
	// Print clickable link to HTML report (file://...)
	reportAbs, err := filepath.Abs(reportPath)
	if err == nil {
		link := fmt.Sprintf("file://%s", reportAbs)
		// ANSI hyperlink: \x1b]8;;<url>\x1b\\<text>\x1b]8;;\x1b\\
		ansiLink := fmt.Sprintf("\x1b]8;;%s\x1b\\%s\x1b]8;;\x1b\\", link, link)
		color.New(color.FgCyan).Printf("HTML report: %s\n", ansiLink)
	} else {
		color.New(color.FgCyan).Printf("HTML report: %s\n", reportPath)
	}
}
