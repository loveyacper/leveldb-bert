// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

package leveldb

import (
	//"bytes"
	"container/list"
	//"fmt"
	"sync"
)

type Version struct {
	// 7层文件集
	files [7][]FileMetaData
}

//func (ver *Version) Get(opt *ReadOptions, key *LookupKey) ([]byte, Status) {

type VersionSet struct {
	options            *Options
	dbname             string
	icmp               *InternalKeyComparator
	nextFileNumber     uint64
	manifestFileNumber uint64
	lastSequence       SequenceNumber
	logNumber          uint64

	// Opened lazily
	//manifestFile WritableFile
	manifestLog *LogWriter
	versions    *list.List
}

func NewVersionSet(dbname string, opts *Options, icmp *InternalKeyComparator) *VersionSet {
	vs := &VersionSet{}
	vs.dbname = dbname
	vs.options = opts
	vs.icmp = icmp
	vs.nextFileNumber = 2
	return vs
}

func (vs *VersionSet) ManifestFileNumber() uint64 {
	return vs.manifestFileNumber
}

func (vs *VersionSet) NewFileNumber() uint64 {
	n := vs.nextFileNumber
	vs.nextFileNumber++
	return n
}
func (vs *VersionSet) SetLastSequence(s SequenceNumber) {
	if s < vs.lastSequence {
		panic("yyy")
	}
	vs.lastSequence = s
}

func (vs *VersionSet) LastSequence() SequenceNumber {
	return vs.lastSequence
}

func (vs *VersionSet) LogNumber() uint64 {
	return vs.logNumber
}

func (vs *VersionSet) MarkFileNumberUsed(number uint64) {
	if vs.nextFileNumber <= number {
		vs.nextFileNumber = number + 1
	}
}

// Apply *edit to the current version to form a new descriptor that
// is both saved to persistent state and installed as the new
// current version.  Will release *mu while actually writing to the file.
// REQUIRES: *mu is held on entry.
// REQUIRES: no other thread concurrently calls LogAndApply()
func (vs *VersionSet) LogAndApply(edit *VersionEdit, mu *sync.Mutex) Status {
	if edit.HasLogNumber {
		ln := edit.LogNumber
		if ln < vs.logNumber || ln >= vs.nextFileNumber {
			panic("xxx")
		}
	}

	edit.SetNextFile(vs.nextFileNumber)
	edit.SetLastSequence(vs.lastSequence)
	// TODO: create new version

	// Initialize new descriptor log file if necessary by creating
	// a temporary file that contains a snapshot of the current version.
	var newManifestName []byte
	if vs.manifestLog == nil {
		manifest := DescriptorFileName(vs.dbname, vs.manifestFileNumber)
		if wfile, s := vs.options.Env.NewWritableFile(manifest); !s.IsOK() {
			return s
		} else {
			vs.manifestLog = NewLogWriter(wfile)
		}
		newManifestName = []byte(manifest)
		// TODO ? WriteSnapshot
	}
	// Unlock during expensive MANIFEST log write
	mu.Unlock()
	// Write new record to MANIFEST log
	s := vs.manifestLog.AddRecord(edit.Encode())
	if s.IsOK() {
		s = vs.manifestLog.Sync()
	}
	if s.IsOK() && len(newManifestName) > 0 {
		// Make "CURRENT" file that points to the new manifest file.
		if err := SetCurrentFile(db.name, vs.manifestFileNumber); err != nil {
			s = NewStatus(IOError, err.Error())
		} else {
			debug.Println("SetCurrentFile to ", newManifestName)
		}
	}
	mu.Lock()

	// TODO Install the new version
	if s.IsOK() {
		vs.logNumber = edit.LogNumber
	} else {
		if len(newManifestName) > 0 {
			vs.manifestLog.Close()
			vs.options.Env.RemoveFile(string(newManifestName))
			vs.manifestLog = nil
		}
	}

	return s
}

// Recover the last saved descriptor from persistent storage.
func (vs *VersionSet) Recover() Status {
	var manifestName []byte
	if file, st := vs.options.Env.NewSequentialFile(CurrentFileName(vs.dbname)); !st.IsOK() {
		return st
	} else {
		var current [32]byte
		data, st := file.Read(32, current[:])
		file.Close()

		if !st.IsOK() {
			return st
		}

		// "MANIFEST-000001" is at least 15 bytes
		if len(data) < 15 {
			panic("wrong CURRENT:" + string(data))
		}

		manifestName = data[:len(data)-1]
	}

	hasLogNumber := false
	hasNextFile := false
	hasLastSequence := false
	var nextFile uint64 = 0
	var lastSequence SequenceNumber = 0
	var logNumber uint64 = 0

	s := NewStatus(OK)
	if file, st := vs.options.Env.NewSequentialFile(vs.dbname + "/" + string(manifestName)); !st.IsOK() {
		return st
	} else {
		debug.Println("Recover read manifest success:", string(manifestName))
		reader := NewLogReader(file)

		// TODO builder version

		var record []byte
		for reader.ReadRecord(&record) {
			debug.Println("VersionSet Record ", len(record))
			var edit VersionEdit
			if st := edit.Decode(record); st.IsOK() {
				if edit.HasComparator && edit.Comparator != vs.icmp.userCmp.Name() {
					s = NewStatus(InvalidArgument, edit.Comparator+" does not match: ", vs.icmp.userCmp.Name())
				}
			}
			// TODO builder Apply
			if edit.HasLogNumber {
				hasLogNumber = true
				logNumber = edit.LogNumber
			}
			if edit.HasNextFileNumber {
				hasNextFile = true
				nextFile = edit.NextFileNumber
			}
			if edit.HasLastSequence {
				hasLastSequence = true
				lastSequence = edit.LastSequence
			}
		}

		reader.Close()
	}

	if s.IsOK() {
		if !hasNextFile {
			s = NewStatus(Corruption, " no meta-nextfile entry in descriptor")
		} else if !hasLogNumber {
			s = NewStatus(Corruption, "no meta-lognumber entry in descriptor")
		} else if !hasLastSequence {
			s = NewStatus(Corruption, "no last-sequence-number entry in descriptor")
		}
	}
	// should never happen
	//MarkFileNumberUsed(log_number); // 第一次运行的时候，log-number是0

	if s.IsOK() {
		//Version* v = new Version(this);
		//builder.SaveTo(v);
		// Install recovered version
		//Finalize(v);
		//AppendVersion(v);

		vs.manifestFileNumber = nextFile //manifest号码只是每次重启open db时设置，运行时不变。
		vs.nextFileNumber = nextFile + 1
		vs.lastSequence = lastSequence
		vs.logNumber = logNumber
		debug.Printf("VersionSet: manifest %v, nextFile %v, last_seq %v, log_num %v",
			vs.manifestFileNumber,
			vs.nextFileNumber,
			vs.lastSequence,
			vs.logNumber)
	}

	return s
}
