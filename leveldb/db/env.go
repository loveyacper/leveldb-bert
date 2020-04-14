// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"io"
)

type Env interface {
	// Create an object that sequentially reads the file with the specified name.
	// On success, return new file and nil status.
	// On failure return nil and non-nil status . If the file does
	// not exist, returns a non-OK status.  Implementations should return a
	// NotFound status when the file does not exist.
	//
	// The returned file will only be accessed by one thread at a time.
	NewSequentialFile(fname string) (SequentialFile, Status)

	// Create an object supporting random-access reads from the file with the
	// specified name. On success, return new file in and nil status.  On failure return nil and
	// non-nil status.  If the file does not exist, returns a non-nil
	// status. Implementations should return a NotFound status when the file does
	// not exist.
	//
	// The returned file may be concurrently accessed by multiple threads.
	NewRandomAccessFile(fname string) (RandomAccessFile, Status)

	// Create an object that writes to a new file with the specified
	// name. Deletes any existing file with the same name and creates a
	// new file. On success, return new file and nil status. On failure return nil and
	// non-nil status.
	//
	// The returned file will only be accessed by one thread at a time.
	NewWritableFile(fname string) (WritableFile, Status)

	// Create an object that either appends to an existing file, or
	// writes to a new file (if the file does not exist to begin with).
	// On success, return new file and nil status. On failure return nil and
	// non-nil status.
	//
	// The returned file will only be accessed by one thread at a time.
	//
	// May return an IsNotSupportedError error if this Env does
	// not allow appending to an existing file. Users of Env (including
	// the leveldb implementation) must be prepared to deal with
	// an Env that does not support appending.
	NewAppendableFile(fname string) (WritableFile, Status)

	// Returns true iff the named file exists.
	FileExists(fname string) bool

	// Return the names of the children of the specified directory.
	// The names are relative to "dir".
	// Original contents of *results are dropped.
	GetChildren(dir string) ([]string, Status)

	// Delete the named file.
	//
	// The default implementation calls DeleteFile, to support legacy Env
	// implementations. Updated Env implementations must override RemoveFile and
	// ignore the existence of DeleteFile. Updated code calling into the Env API
	// must call RemoveFile instead of DeleteFile.
	//
	// A future release will remove DeleteDir and the default implementation of
	// RemoveDir.
	RemoveFile(fname string) Status

	// Create the specified directory.
	CreateDir(dirname string) Status

	// Delete the specified directory.
	//
	// The default implementation calls DeleteDir, to support legacy Env
	// implementations. Updated Env implementations must override RemoveDir and
	// ignore the existence of DeleteDir. Modern code calling into the Env API
	// must call RemoveDir instead of DeleteDir.
	//
	// A future release will remove DeleteDir and the default implementation of
	// RemoveDir.
	RemoveDir(dirname string) Status

	// Store the size of fname in *file_size.
	GetFileSize(fname string) (int64, Status)

	// Rename file src to target.
	RenameFile(src, target string) Status

	// Lock the specified file. Used to prevent concurrent access to
	// the same db by multiple processes.  On failure, return nil and bad status.
	//
	// On success, return the object that represents the
	// acquired lock and OK.  The caller should call
	// UnlockFile(lock) to release the lock.  If the process exits,
	// the lock will be automatically released.
	//
	// If somebody else already holds the lock, finishes immediately
	// with a failure.  I.e., this call does not wait for existing locks
	// to go away.
	//
	// May create the named file if it does not already exist.
	LockFile(fname string) (FileLock, Status)

	// Release the lock acquired by a previous successful call to LockFile.
	// REQUIRES: lock was returned by a successful LockFile() call
	// REQUIRES: lock has not already been unlocked.
	UnlockFile(lock FileLock) Status
}

// A file abstraction for reading sequentially through a file
type SequentialFile interface {
	// Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
	// written by this routine.  Return the data that was
	// read (including if fewer than "n" bytes were successfully read).
	// If an error was encountered, returns a non-OK status.
	//
	// REQUIRES: External synchronization
	Read(n int, scratch []byte) ([]byte, Status)

	// Skip "n" bytes from the file. This is guaranteed to be no
	// slower that reading the same data, but may be faster.
	//
	// If end of file is reached, skipping will stop at the end of the
	// file, and Skip will return OK.
	//
	// REQUIRES: External synchronization
	Skip(n int64) Status

	io.Closer
}

// A file abstraction for randomly reading the contents of a file.
type RandomAccessFile interface {
	// Read up to "n" bytes from the file starting at "offset".
	// "scratch[0..n-1]" may be written by this routine.  Return
	// the data that was read (including if fewer than "n" bytes were
	// successfully read).
	// If an error was encountered, returns a non-OK status.
	//
	// Safe for concurrent use by multiple threads.
	Read(offset int64, n int, scratch []byte) ([]byte, Status)

	io.Closer
}

// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
type WritableFile interface {
	Append(data []byte) Status
	Flush() Status
	Sync() Status
	io.Closer
}

// Identifies a locked file.
type FileLock interface {
	io.Closer
}

// Return a default environment suitable for the current operating
// system.  Sophisticated users may wish to provide their own Env
// implementation instead of relying on this default environment.
//
// The result of Default() belongs to leveldb and must never be deleted.
var posixEnv Env

func init() {
	posixEnv = NewPosixEnv()
}

func DefaultEnv() Env {
	return posixEnv
}
