// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

// Options to control the behavior of a database (passed to DB::Open)
type Options struct {
	// -------------------
	// Parameters that affect behavior

	// Comparator used to define the order of keys in the table.
	// Default: a comparator that uses lexicographic byte-wise ordering
	//
	// REQUIRES: The client must ensure that the comparator supplied
	// here has the same name and orders keys *exactly* the same as the
	// comparator provided to previous open calls on the same DB.
	Comp Comparator

	// If true, the database will be created if it is missing.
	// Default: false
	CreateIfMissing bool

	// If true, an error is raised if the database already exists.
	// Default: false
	ErrorIfExists bool

	// Use the specified object to interact with the environment,
	// e.g. to read/write files, schedule background work, etc.
	// Default: DefaultEnv()
	Env Env
}

// Options that control write operations
type WriteOptions struct {
	// If true, the write will be flushed from the operating system
	// buffer cache (by calling WritableFile.Sync()) before the write
	// is considered complete.  If this flag is true, writes will be
	// slower.
	//
	// If this flag is false, and the machine crashes, some recent
	// writes may be lost.  Note that if it is just the process that
	// crashes (i.e., the machine does not reboot), no writes will be
	// lost even if sync==false.
	//
	// In other words, a DB write with sync==false has similar
	// crash semantics as the "write()" system call.  A DB write
	// with sync==true has similar crash semantics to a "write()"
	// system call followed by "fsync()".
	//
	// Default: false
	Sync bool
}

// Options that control read operations
type ReadOptions struct {
	// If true, all data read from underlying storage will be
	// verified against corresponding checksums.
	// Default: false
	VerifyChecksums bool

	// Should the data read for this iteration be cached in memory?
	// Callers may wish to set this field to false for bulk scans.
	// Default: true
	FillCache bool

	// If "snapshot" is <= kMaxSequenceNumber, read as of the supplied snapshot
	// (which must belong to the DB that is being read and which must
	// not have been released).  If "snapshot" is beyond kMaxSequenceNumber, use an implicit
	// snapshot of the state at the beginning of this read operation.
	// Default: kMaxSequenceNumber+1
	Snapshot SequenceNumber
}

func NewReadOptions() *ReadOptions {
	return &ReadOptions{VerifyChecksums: false, FillCache: true, Snapshot: kMaxSequenceNumber + 1}
}

func NewOptions() *Options {
	opt := &Options{}
	opt.Comp = NewBytewiseComparator()
	opt.CreateIfMissing = false
	opt.ErrorIfExists = false

	return opt
}

func NewCreateIfMissingOptions() *Options {
	opt := NewOptions()
	opt.CreateIfMissing = true

	return opt
}
