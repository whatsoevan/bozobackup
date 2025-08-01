// bozobackup: Incremental, deduplicating photo/video backup tool with HTML reporting.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	bar := progressbar.NewOptions(
		len(files),
		progressbar.OptionSetDescription("Processing"),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionSetWidth(20),
		progressbar.OptionSetPredictTime(true), // ETA
		progressbar.OptionSetElapsedTime(true), // Elapsed
		progressbar.OptionClearOnFinish(),
	)
	var copied, duplicates, errors int
	var errorList []string
	var copiedFiles [][2]string    // [][src, dst] for HTML report
	var duplicateFiles [][2]string // [][src, dst] for HTML report
	var skippedFiles []SkippedFile // Skipped files and reasons for HTML report
	var totalCopiedSize int64
	var filesToCopy []string // Used for free space estimation

	// First pass: determine which files will be copied and their total size
	for _, file := range files {
		ext := strings.ToLower(filepath.Ext(file))
		if !allowedExtensions[ext] {
			skippedFiles = append(skippedFiles, SkippedFile{Path: file, Reason: "filtered (extension)"})
			continue
		}
		info, err := os.Stat(file)
		if err != nil {
			skippedFiles = append(skippedFiles, SkippedFile{Path: file, Reason: fmt.Sprintf("stat error: %v", err)})
			continue
		}
		if incremental && minMtime > 0 && info.ModTime().Unix() <= minMtime {
			skippedFiles = append(skippedFiles, SkippedFile{Path: file, Reason: "old (not newer than last backup)"})
			continue
		}
		date := getFileDate(file)
		if date.IsZero() {
			skippedFiles = append(skippedFiles, SkippedFile{Path: file, Reason: "no date found"})
			continue
		}
		monthFolder := date.Format("2006-01")
		destMonthDir := filepath.Join(destDir, monthFolder)
		os.MkdirAll(destMonthDir, 0755)
		destFile := filepath.Join(destMonthDir, filepath.Base(file))
		if _, err := os.Stat(destFile); err == nil {
			skippedFiles = append(skippedFiles, SkippedFile{Path: file, Reason: "already present at destination"})
			continue
		}
		filesToCopy = append(filesToCopy, file)
		totalCopiedSize += info.Size()
	}

	// Check free space before copying
	dbEstimate := estimateDBSize(len(filesToCopy))
	requiredSpace := totalCopiedSize + dbEstimate
	free, err := getFreeSpace(destDir)
	if err != nil {
		color.New(color.FgRed).Printf("[FATAL] Could not determine free space for '%s': %v\n", destDir, err)
		os.Exit(1)
	}
	if free < uint64(requiredSpace) {
		color.New(color.FgRed).Printf("[FATAL] Not enough free space in destination. Required: %.2f MB, Available: %.2f MB\n",
			float64(requiredSpace)/(1024*1024), float64(free)/(1024*1024))
		os.Exit(1)
	}

	// Second pass: process files (copy, dedup, record, report)
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
		ext := strings.ToLower(filepath.Ext(file))
		if !allowedExtensions[ext] {
			bar.Add(1)
			continue
		}
		info, err := os.Stat(file)
		if err != nil {
			// Only log errors to errorList, not terminal
			errorList = append(errorList, fmt.Sprintf("%s: stat error: %v", file, err))
			bar.Add(1)
			continue
		}
		if incremental && minMtime > 0 && info.ModTime().Unix() <= minMtime {
			bar.Add(1)
			continue
		}
		date := getFileDate(file)
		if date.IsZero() {
			bar.Add(1)
			continue
		}
		monthFolder := date.Format("2006-01")
		destMonthDir := filepath.Join(destDir, monthFolder)
		os.MkdirAll(destMonthDir, 0755)
		destFile := filepath.Join(destMonthDir, filepath.Base(file))
		if _, err := os.Stat(destFile); err == nil {
			bar.Add(1)
			continue
		}
		// Only now compute hash and check for duplicates
		size, mtime := getFileStat(file)
		hash := getFileHash(file)
		if hash == "" {
			// Only log errors to errorList, not terminal
			errorList = append(errorList, fmt.Sprintf("%s: hash error", file))
			errors++
			bar.Add(1)
			continue
		}
		if fileAlreadyProcessed(db, hash) {
			duplicates++
			duplicateFiles = append(duplicateFiles, [2]string{file, destFile})
			bar.Add(1)
			continue
		}
		if err := copyFileAtomic(ctx, file, destFile); err != nil {
			// Only log errors to errorList, not terminal
			errorList = append(errorList, fmt.Sprintf("%s: copy error: %v", file, err))
			errors++
			bar.Add(1)
			if ctx.Err() != nil {
				break
			}
			continue
		}
		insertFileRecord(db, file, destFile, hash, size, mtime)
		copied++
		copiedFiles = append(copiedFiles, [2]string{file, destFile})
		bar.Add(1)
	}

cleanup:
	// Log any errors from walking the file tree
	for _, walkErr := range walkErrors {
		errorList = append(errorList, fmt.Sprintf("walk error: %v", walkErr))
	}

	totalTime := time.Since(startTime)

	// Generate HTML report with all results
	writeHTMLReport(reportPath, copiedFiles, duplicateFiles, skippedFiles, errorList, totalCopiedSize, totalTime)

	// Print a summary and check accounting
	totalFound := len(files)
	totalCopied := len(copiedFiles)
	totalSkipped := len(skippedFiles)
	totalDuplicates := len(duplicateFiles)
	totalErrors := errors + len(walkErrors)
	totalAccounted := totalCopied + totalSkipped + totalDuplicates + totalErrors

	fmt.Println()
	color.New(color.FgGreen).Printf("Copied: %d, ", totalCopied)
	color.New(color.FgYellow).Printf("Skipped: %d, Duplicates: %d, ", totalSkipped, totalDuplicates)
	color.New(color.FgRed).Printf("Errors: %d, ", totalErrors)
	fmt.Printf("Total Found: %d\n", totalFound)
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
