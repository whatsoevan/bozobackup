// bozobackup: File processing pipeline structures for Phase 1 refactor
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// FileState represents the explicit state of a file during processing
// This eliminates the classification ambiguity between passes
type FileState int

const (
	// File was successfully copied
	StateCopied FileState = iota
	
	// File was skipped for various reasons
	StateSkippedExtension    // Extension not in allowedExtensions
	StateSkippedIncremental  // File older than last backup (incremental mode)
	StateSkippedDate         // Could not extract valid date from file
	StateSkippedDestExists   // Destination file already exists
	
	// File is a duplicate based on hash
	StateDuplicateHash       // Hash already exists in database
	
	// Errors during processing
	StateErrorStat          // Error calling os.Stat()
	StateErrorDate          // Error extracting date metadata
	StateErrorHash          // Error computing file hash
	StateErrorCopy          // Error copying file
	StateErrorWalk          // Error during directory walking
)

// String returns human-readable state names for reporting
func (s FileState) String() string {
	switch s {
	case StateCopied:
		return "copied"
	case StateSkippedExtension:
		return "skipped (extension)"
	case StateSkippedIncremental:
		return "skipped (incremental)"
	case StateSkippedDate:
		return "skipped (no date)"
	case StateSkippedDestExists:
		return "skipped (destination exists)"
	case StateDuplicateHash:
		return "duplicate (hash exists)"
	case StateErrorStat:
		return "error (stat)"
	case StateErrorDate:
		return "error (date extraction)"
	case StateErrorHash:
		return "error (hash computation)"
	case StateErrorCopy:
		return "error (copy failed)"
	case StateErrorWalk:
		return "error (walk failed)"
	default:
		return "unknown"
	}
}

// FileCandidate represents a file being evaluated for backup
// This structure caches expensive operations like os.Stat() and date extraction
// to eliminate redundant I/O between the two-pass system
type FileCandidate struct {
	// Basic file information
	Path      string      // Full source path
	Info      os.FileInfo // Cached os.Stat() result (expensive, called once)
	Extension string      // Normalized lowercase extension (e.g., ".jpg")
	
	// Extracted metadata (cached to avoid expensive re-computation)
	Date     time.Time // Extracted from EXIF/video metadata or mtime fallback
	DateErr  error     // Any error from date extraction
	
	// Destination information
	DestDir  string // Base destination directory
	DestPath string // Full computed destination path (YYYY-MM/filename)
	
	// Processing metadata (computed on-demand)
	Hash    string // SHA256 hash (computed during copy or when needed for duplicate check)
	HashErr error  // Any error from hash computation
}

// NewFileCandidate creates a FileCandidate with basic information populated
// This performs the expensive os.Stat() call once and caches the result
func NewFileCandidate(path, destDir string) (*FileCandidate, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	
	ext := strings.ToLower(filepath.Ext(path))
	
	candidate := &FileCandidate{
		Path:      path,
		Info:      info,
		Extension: ext,
		DestDir:   destDir,
	}
	
	return candidate, nil
}

// EnsureDate extracts and caches the file date if not already done
// This is expensive for video files (ffprobe) so we cache the result
func (fc *FileCandidate) EnsureDate() {
	if !fc.Date.IsZero() || fc.DateErr != nil {
		return // Already extracted
	}
	
	fc.Date, fc.DateErr = fc.extractFileDate()
}

// EnsureDestPath computes and caches the destination path based on extracted date
func (fc *FileCandidate) EnsureDestPath() error {
	if fc.DestPath != "" {
		return nil // Already computed
	}
	
	fc.EnsureDate()
	if fc.DateErr != nil {
		return fc.DateErr
	}
	
	if fc.Date.IsZero() {
		return nil // No valid date, can't compute destination
	}
	
	monthFolder := fc.Date.Format("2006-01")
	destMonthDir := filepath.Join(fc.DestDir, monthFolder)
	fc.DestPath = filepath.Join(destMonthDir, filepath.Base(fc.Path))
	
	return nil
}

// EnsureHash computes and caches the file hash if not already done
// Only used for duplicate checking when file won't be copied
func (fc *FileCandidate) EnsureHash() {
	if fc.Hash != "" || fc.HashErr != nil {
		return // Already computed
	}

	fc.Hash = getFileHash(fc.Path)
	if fc.Hash == "" {
		fc.HashErr = fmt.Errorf("failed to compute hash")
	}
}

// SetHash sets the hash (used when computed during streaming copy)
func (fc *FileCandidate) SetHash(hash string) {
	fc.Hash = hash
	fc.HashErr = nil
}

// extractFileDate performs the actual date extraction using the comprehensive metadata system
func (fc *FileCandidate) extractFileDate() (time.Time, error) {
	// Use the global metadata registry for comprehensive extraction
	result := metadataRegistry.ExtractBestDate(fc.Path)
	
	if result.Error != nil || result.Date.IsZero() {
		// Fallback to file modification time
		return fc.Info.ModTime(), result.Error
	}
	
	return result.Date, nil
}

// ProcessingDecision encapsulates the decision of whether/how a file should be processed
// This eliminates the inconsistent classification between passes
type ProcessingDecision struct {
	State      FileState // Explicit state (copied, skipped, duplicate, error)
	Reason     string    // Human-readable explanation for reporting
	ShouldCopy bool      // Clear boolean: should this file be copied?
	
	// Additional context for decision
	Priority   int       // Processing priority (for future parallel processing)
	EstimatedSize int64  // Expected bytes to copy (for progress estimation)
}

// ProcessingResult tracks the outcome of file operations
// This replaces the scattered outcome tracking in multiple arrays
type ProcessingResult struct {
	Candidate   *FileCandidate // Original file candidate
	Decision    ProcessingDecision // Decision that was made
	FinalState  FileState     // Actual final state after processing
	
	// Execution details
	Error        error         // Any error that occurred during processing
	BytesCopied  int64        // Actual bytes copied (0 if skipped/error)
	TimeTaken    time.Duration // Time spent processing this file
	
	// Database tracking
	DBInserted   bool         // Whether file record was inserted into DB
	
	// Timestamps
	StartTime    time.Time    // When processing started
	EndTime      time.Time    // When processing completed
}

// NewProcessingResult creates a result with timing started
func NewProcessingResult(candidate *FileCandidate, decision ProcessingDecision) *ProcessingResult {
	return &ProcessingResult{
		Candidate:  candidate,
		Decision:   decision,
		FinalState: decision.State, // Initialize with decision state
		StartTime:  time.Now(),
	}
}

// Complete marks the result as finished and records timing
func (pr *ProcessingResult) Complete(finalState FileState, err error, bytesCopied int64) {
	pr.EndTime = time.Now()
	pr.TimeTaken = pr.EndTime.Sub(pr.StartTime)
	pr.FinalState = finalState
	pr.Error = err
	pr.BytesCopied = bytesCopied
}

// IsSuccess returns true if the file was processed successfully (copied or legitimately skipped)
func (pr *ProcessingResult) IsSuccess() bool {
	switch pr.FinalState {
	case StateCopied, StateSkippedExtension, StateSkippedIncremental, 
		 StateSkippedDate, StateSkippedDestExists, StateDuplicateHash:
		return true
	default:
		return false
	}
}

// IsError returns true if processing failed due to an error
func (pr *ProcessingResult) IsError() bool {
	switch pr.FinalState {
	case StateErrorStat, StateErrorDate, StateErrorHash, StateErrorCopy, StateErrorWalk:
		return true
	default:
		return false
	}
}

// classifyAndProcessFile performs unified file classification and processing with database optimization
// Uses pre-loaded hash cache for O(1) duplicate checking and batch database operations
func classifyAndProcessFile(ctx context.Context, candidate *FileCandidate, hashCache *HashCache, incremental bool, minMtime int64) *ProcessingResult {
	// Get processing decision using database-optimized evaluation logic
	decision := evaluateFileForBackup(candidate, hashCache, incremental, minMtime)
	result := NewProcessingResult(candidate, decision)

	// If decision says don't copy, we're done - return with decision state
	if !decision.ShouldCopy {
		result.Complete(decision.State, nil, 0)
		return result
	}

	// Decision says we should copy - copy file with timestamp preservation
	var finalState FileState
	var bytesCopied int64
	var copyErr error

	if ctx.Err() != nil {
		// Context cancelled before we could copy
		finalState = StateErrorCopy
		copyErr = ctx.Err()
	} else {
		// Copy file (hash was already computed during evaluation)
		copyErr = copyFileWithTimestamps(ctx, candidate.Path, candidate.DestPath)
		if copyErr != nil {
			finalState = StateErrorCopy
		} else {
			// Copy succeeded - add to batch for database insertion
			hashCache.AddToBatch(candidate.Path, candidate.DestPath, candidate.Hash,
							   candidate.Info.Size(), candidate.Info.ModTime().Unix())
			finalState = StateCopied
			bytesCopied = candidate.Info.Size()
			result.DBInserted = true
		}
	}

	result.Complete(finalState, copyErr, bytesCopied)
	return result
}

// AccountingSummary provides bulletproof accounting from ProcessingResult collection
// This eliminates the need for manual counters and band-aid fixes
type AccountingSummary struct {
	// Counts by final state
	Copied     int
	Skipped    int
	Duplicates int
	Errors     int

	// File lists for HTML report generation
	CopiedFiles    [][2]string    // [src, dst] pairs
	SkippedFiles   []SkippedFile  // Files skipped with reasons
	DuplicateFiles [][2]string    // [src, dst] pairs for duplicates
	ErrorList      []string       // Error messages

	// Statistics
	TotalBytes   int64  // Total bytes copied
	TotalFiles   int    // Total files processed
	WalkErrors   int    // Directory walking errors
}

// SkippedFile represents a file that was skipped during backup
type SkippedFile struct {
	Path   string
	Reason string
}

// GenerateAccountingSummary creates a complete accounting summary from ProcessingResult collection
// This provides perfect accounting with no possibility of inconsistencies or unaccounted files
func GenerateAccountingSummary(results []*ProcessingResult, walkErrors []error) AccountingSummary {
	summary := AccountingSummary{
		TotalFiles: len(results),
		WalkErrors: len(walkErrors),
	}

	// Process each result and categorize by final state
	for _, result := range results {
		switch result.FinalState {
		case StateCopied:
			summary.Copied++
			summary.CopiedFiles = append(summary.CopiedFiles, [2]string{
				result.Candidate.Path,
				result.Candidate.DestPath,
			})
			summary.TotalBytes += result.BytesCopied

		case StateDuplicateHash:
			summary.Duplicates++
			summary.DuplicateFiles = append(summary.DuplicateFiles, [2]string{
				result.Candidate.Path,
				result.Candidate.DestPath,
			})

		case StateSkippedExtension, StateSkippedIncremental, StateSkippedDate, StateSkippedDestExists:
			summary.Skipped++
			summary.SkippedFiles = append(summary.SkippedFiles, SkippedFile{
				Path:   result.Candidate.Path,
				Reason: result.FinalState.String(),
			})

		case StateErrorStat, StateErrorDate, StateErrorHash, StateErrorCopy:
			summary.Errors++
			errorMsg := fmt.Sprintf("%s: %v", result.Candidate.Path, result.Error)
			if result.Error == nil {
				errorMsg = fmt.Sprintf("%s: %s", result.Candidate.Path, result.FinalState.String())
			}
			summary.ErrorList = append(summary.ErrorList, errorMsg)

		case StateErrorWalk:
			// Walk errors are handled separately in walkErrors parameter
			summary.Errors++
		}
	}

	// Add walk errors to error list
	for _, walkErr := range walkErrors {
		summary.ErrorList = append(summary.ErrorList, fmt.Sprintf("walk error: %v", walkErr))
	}
	summary.Errors += len(walkErrors)

	return summary
}

// Validate checks that accounting is perfect (no missing files)
func (as *AccountingSummary) Validate() error {
	accountedFiles := as.Copied + as.Skipped + as.Duplicates + as.Errors - as.WalkErrors
	if accountedFiles != as.TotalFiles {
		return fmt.Errorf("accounting mismatch: processed %d files but accounted for %d",
			as.TotalFiles, accountedFiles)
	}
	return nil
}