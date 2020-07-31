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
	"bytes"
	"container/list"
	"sort"
	"sync"
)

type Version struct {
	// 7层文件集
	files [NumLevels][]FileMetaData
}

func (ver *Version) String() string {
	buf := new(bytes.Buffer)
	for _, files := range ver.files {
		for _, f := range files {
			buf.WriteString(f.String())
		}
	}
	return string(buf.Bytes())
}

func (ver *Version) NumFiles(level int) int {
	return len(ver.files[level])
}

func (ver *Version) Get(opt *ReadOptions, lkey *LookupKey) ([]byte, Status) {
	return nil, NewStatus(OK)
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.

type VersionLevelState struct {
	deletedFiles map[uint64]struct{}
	addedFiles   []FileMetaData
}

type VersionBuilder struct {
	vset   *VersionSet
	base   *Version
	levels [NumLevels]VersionLevelState
}

func BySmallestKey(cmp Comparator) func(f1, f2 *FileMetaData) bool {
	comparator := cmp
	return func(f1, f2 *FileMetaData) bool {
		r := comparator.Compare(f1.Smallest.Encode(), f2.Smallest.Encode())
		if r != 0 {
			return r < 0
		}

		return f1.Number < f2.Number // level-0 files
	}
}

// Apply all of the edits in *edit to the current state.
func (vb *VersionBuilder) Apply(edit *VersionEdit) {
	/*
	   // Update compaction pointers
	   for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
	     const int level = edit->compact_pointers_[i].first;
	     vset_->compact_pointer_[level] =
	         edit->compact_pointers_[i].second.Encode().ToString();
	   }
	*/

	// Delete files
	for _, f := range edit.DeletedFiles {
		if vb.levels[f.Level].deletedFiles == nil {
			vb.levels[f.Level].deletedFiles = make(map[uint64]struct{})
		}

		vb.levels[f.Level].deletedFiles[f.Number] = struct{}{}
	}

	// Add new files
	for _, f := range edit.NewFiles {
		level := f.Level
		// We arrange to automatically compact this file after
		// a certain number of seeks.  Let's assume:
		//   (1) One seek costs 10ms
		//   (2) Writing or reading 1MB costs 10ms (100MB/s)
		//   (3) A compaction of 1MB does 25MB of IO:
		//         1MB read from this level
		//         10-12MB read from next level (boundaries may be misaligned)
		//         10-12MB written to next level
		// This implies that 25 seeks cost the same as the compaction
		// of 1MB of data.  I.e., one seek costs approximately the
		// same as the compaction of 40KB of data.  We are a little
		// conservative and allow approximately one seek for every 16KB
		// of data before triggering a compaction.
		// f->allowed_seeks = (f->file_size / 16384);
		//if (f->allowed_seeks < 100) f->allowed_seeks = 100; // 文件在被压缩之前，允许访问多少次
		if vb.levels[level].deletedFiles != nil {
			delete(vb.levels[level].deletedFiles, f.Number)
		}
		debug.Println("versionBuilder added file", f.Number)
		vb.levels[level].addedFiles = append(vb.levels[level].addedFiles, f)
	}
}

// Save the current state in *v.
func (vb *VersionBuilder) SaveTo() *Version {
	var v Version
	cmpFunc := BySmallestKey(vb.vset.icmp)

	for level := 0; level < NumLevels; level++ {
		// Merge the set of added files with the set of pre-existing files.
		// Drop any deleted files.  Store the result in *v.
		baseFiles := vb.base.files[level]
		added := vb.levels[level].addedFiles

		files := make([]FileMetaData, 0, len(baseFiles)+len(added))
		files = append(files, baseFiles...)
		files = append(files, added...)
		sort.Slice(files, func(i, j int) bool {
			return cmpFunc(&files[i], &files[j])
		})

		for _, f := range files {
			vb.MaybeAddFile(&v, level, &f)
		}
	}

	return &v
}

func (vb *VersionBuilder) MaybeAddFile(v *Version, level int, f *FileMetaData) {
	if vb.levels[f.Level].deletedFiles != nil {
		if _, ok := vb.levels[level].deletedFiles[f.Number]; ok {
			return
		}
	}

	// debug
	if level > 0 && len(v.files[level]) > 0 {
		files := v.files[level]
		if vb.vset.icmp.Compare(files[len(files)-1].Largest.Encode(), f.Smallest.Encode()) >= 0 {
			debug.Panicf("VersionBuilder.MaybeAddFile level %v, prev largest %v >= smallest %v",
				level, files[len(files)-1].Largest.Encode(), f.Smallest.Encode())
		}
	}

	debug.Println("MaybeAddFile level", level, " number", f.Number)
	v.files[level] = append(v.files[level], *f)
}

type VersionSet struct {
	options *Options
	dbname  string
	icmp    *InternalKeyComparator
	// ensure file number monotonic increase
	nextFileNumber uint64
	// 该字段不会持久化，在recover过程中设置，用来将来WriteSnapshot()时生成新的manifest文件名
	manifestFileNumber uint64
	/*
	   该seq字段，仅仅在没有wal-log文件的情况下有用。比如刚刚minor压缩了memtable，且此时没有写请求，那vs这里记录的seq就有用。
	   如果生成了新的memtable，且有数据，那么重启恢复的时候，seq是从wal里更新，因为wal里一定是最新数据，seq最大。
	   但是注意，在minor压缩时，logAndApply所使用的VersionEdit对象，仅仅含有新生成的ldb文件和淘汰的wal文件，并没有seq。
	   seq使用的则是vs的成员变量，这个变量会被writer线程更新的。也就是说，后台压缩线程执行写入manifest的seq并不一定是ldb文件中
	   最大的seq，而是更大。因为有writer线程更新了seq，其对应的数据也写入了wal。
	   总之，记录在manifest的seq只是个保底的作用，只有在wal文件为空的情况下有用。只要wal非空，在重启时执行recovery log，
	   就会把seq更新，因为wal log中的seq一定更大。
	*/
	lastSequence SequenceNumber
	logNumber    uint64

	// Opened lazily
	manifestLog *LogWriter
	versions    *list.List
	current     *Version
}

func NewVersionSet(dbname string, opts *Options, icmp *InternalKeyComparator) *VersionSet {
	vs := &VersionSet{}
	vs.dbname = dbname
	vs.options = opts
	vs.icmp = icmp
	vs.nextFileNumber = 2
	vs.versions = list.New()
	debug.Println("NewVersionSet, so append a dummy version")
	vs.AppendVersion(&Version{})
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

func (vs *VersionSet) ReuseFileNumber(number uint64) {
	if vs.nextFileNumber == number+1 {
		vs.nextFileNumber = number
	}
}

func (vs *VersionSet) Current() *Version {
	return vs.current
}

func (vs *VersionSet) AppendVersion(v *Version) {
	// Make "v" current
	if vs.current == v {
		debug.Panicln("AppendVersion duplicate with current")
	}

	vs.current = v
	vs.versions.PushBack(v)
	debug.Println("AppendVersion: ", v)
}

func (vs *VersionSet) WriteSnapshot() Status {
	// TODO: Break up into multiple records to reduce memory usage on recovery?

	debug.Println("-----> Begin versionSet.WriteSnapshot")
	defer debug.Println("<----- End versionSet.WriteSnapshot")

	// Save metadata
	var edit VersionEdit
	edit.SetComparatorName(vs.icmp.userCmp.Name())

	/*
	   // Save compaction pointers
	   for (int level = 0; level < config::kNumLevels; level++) {
	       if (!compact_pointer_[level].empty()) {
	         InternalKey key;
	         key.DecodeFrom(compact_pointer_[level]);
	         edit.SetCompactPointer(level, key);
	       }
	   }
	*/

	// Save files
	for level, files := range vs.current.files {
		for _, f := range files {
			edit.AddFile(level, f.Number, f.FileSize, f.Smallest, f.Largest)
		}
	}

	return vs.manifestLog.AddRecord(edit.Encode())
}

func (vs *VersionSet) LiveFiles() []uint64 {
	if vs.versions == nil {
		return []uint64{}
	}

	var res []uint64

	for v := vs.versions.Front(); v != nil; v = v.Next() {
		v := v.Value.(*Version)
		for _, files := range v.files {
			for _, f := range files {
				res = append(res, f.Number)
			}
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i] < res[j]
	})

	return res
}

// Apply *edit to the current version to form a new descriptor that
// is both saved to persistent state and installed as the new
// current version.  Will release *mu while actually writing to the file.
// REQUIRES: *mu is held on entry.
// REQUIRES: no other thread concurrently calls LogAndApply()
func (vs *VersionSet) LogAndApply(edit *VersionEdit, mu *sync.Mutex) Status {
	debug.Println("-----> Begin versionSet.LogAndApply")
	defer debug.Println("<----- End versionSet.LogAndApply")

	if edit.HasLogNumber {
		ln := edit.LogNumber
		if ln < vs.logNumber || ln >= vs.nextFileNumber {
			debug.Panicf("LogAndApply: wrong logNumber %v, should between [%v, %v)", ln, vs.logNumber, vs.nextFileNumber)
		}
	} else {
		edit.SetLogNumber(vs.logNumber)
	}

	edit.SetNextFile(vs.nextFileNumber)
	edit.SetLastSequence(vs.lastSequence)

	// create new version
	var vb VersionBuilder
	vb.vset = vs
	vb.base = vs.current
	vb.Apply(edit)
	newv := vb.SaveTo() // current_ + edit = v
	//vs.Finalize(newv) // 计算哪一层需要压缩

	// Initialize new descriptor log file if necessary by creating
	// a temporary file that contains a snapshot of the current version.
	var newManifestName []byte
	if vs.manifestLog == nil {
		manifest := DescriptorFileName(vs.dbname, vs.manifestFileNumber)
		if wfile, s := vs.options.Env.NewWritableFile(manifest); !s.IsOK() {
			return s
		} else {
			vs.manifestLog = NewLogWriter(wfile)
			// WriteSnapshot,相当于rewrite，因为manifest是由很多个修改记录(versionEdit)串联而成，
			// 当db重启的时候，执行一次rewrite，合成一个version edit记录。
			s = vs.WriteSnapshot()
			newManifestName = []byte(manifest)

			debug.Println("New manifest file", manifest)
		}
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
		if err := SetCurrentFile(vs.dbname, vs.manifestFileNumber); err != nil {
			s = NewStatus(IOError, err.Error())
		} else {
			debug.Println("LogAndApply SetCurrentFile to ", string(newManifestName))
		}
	}
	mu.Lock()

	// TODO Install the new version
	if s.IsOK() {
		vs.AppendVersion(newv)
		vs.logNumber = edit.LogNumber
	} else {
		if len(newManifestName) > 0 {
			vs.manifestLog.Close()
			vs.options.Env.RemoveFile(string(newManifestName))
			vs.manifestLog = nil
			debug.Println("Error logAndApply remove manifest ", newManifestName)
		}
	}

	return s
}

// Recover the last saved descriptor from persistent storage.
func (vs *VersionSet) Recover() Status {
	debug.Println("-----> Begin versionSet.Recover")
	defer debug.Println("<----- End versionSet.Recover")

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
			debug.Panicln("wrong CURRENT contents:" + string(data))
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

	// builder version
	var vb VersionBuilder
	vb.vset = vs
	vb.base = vs.current

	if file, st := vs.options.Env.NewSequentialFile(vs.dbname + "/" + string(manifestName)); !st.IsOK() {
		return st
	} else {
		debug.Println("versionRecover read manifest:", string(manifestName))
		reader := NewLogReader(file)

		var record []byte
		for reader.ReadRecord(&record) {
			var edit VersionEdit
			if st := edit.Decode(record); st.IsOK() {
				if edit.HasComparator && edit.Comparator != vs.icmp.userCmp.Name() {
					s = NewStatus(InvalidArgument, edit.Comparator+" does not match: ", vs.icmp.userCmp.Name())
				}

				debug.Printf("VersionSet.Recover one versionEdit: %v", edit.String())
			}

			if s.IsOK() {
				vb.Apply(&edit)
			}

			// update info because numbers are monotonic, the newer edit will override previous
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
		v := vb.SaveTo()
		// Install recovered version
		//Finalize(v);
		vs.AppendVersion(v)

		vs.manifestFileNumber = nextFile //manifest号码只是每次重启opendb时设置，运行时不变。
		// 这里是为了后面rewrite manifest文件留下伏笔。逻辑很割裂，这块难以看出来
		// TODO 看能否改善下这块逻辑，跨越了几个函数，不好懂
		// 另外，manifest文件在运行期间是不断增长的，只有重启db才会做rewrite压缩。也会导致重启db的recover过程缓慢。

		vs.nextFileNumber = nextFile + 1 // nextFile被分配给manifest了，因为manifest需要一次rewrite，生成新的manifest文件
		vs.lastSequence = lastSequence
		vs.logNumber = logNumber
		debug.Printf("VersionSet.Recover done: manifest %v, nextFile %v, last_seq %v, log_num %v",
			vs.manifestFileNumber,
			vs.nextFileNumber,
			vs.lastSequence,
			vs.logNumber)
	}

	return s
}
