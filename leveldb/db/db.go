package leveldb

import (
	"container/list"
	"os"
	"sort"
	"sync"
)

type DB struct {
	sync.Mutex

	options            *Options
	name               string
	mem                *MemTable
	imm                *MemTable
	internalComparator *InternalKeyComparator

	writers    *list.List
	writeBatch *WriteBatch

	logNumber uint64
	log       *LogWriter

	bgCV       *sync.Cond
	bgError    Status
	compacting bool

	//目前是单线程压缩，我觉得没有pendingOutput的必要
	//pendingOutput map[uint64]struct {} // 保存正在minor生成的ldb文件，免得误被后台删除

	versions *VersionSet

	lock FileLock
}

func NewDB(opt *Options) *DB {
	db := &DB{}
	db.options = opt
	db.internalComparator = &InternalKeyComparator{opt.Comp}
	db.writers = list.New()
	db.writeBatch = NewWriteBatch()

	db.compacting = false
	db.bgCV = sync.NewCond(&db.Mutex)
	db.bgError = NewStatus(OK)

	db.options.Comp = db.internalComparator
	return db
}

func Open(opt *Options, dbname string) (*DB, Status) {
	if opt.Env == nil {
		opt.Env = DefaultEnv()
	}

	opt.Env.CreateDir(dbname)

	// init info logger stuff
	if f, err := os.OpenFile(InfoLogFileName(dbname), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
		initLogger(f)
	} else {
		initLogger(os.Stdout)
	}

	db := NewDB(opt)
	if fl, st := opt.Env.LockFile(LockFileName(dbname)); st.IsOK() {
		db.lock = fl
	} else {
		debug.Println(dbname, " is already locked", st)
		return nil, NewStatus(IOError, dbname+" already locked, maybe some other db instance is working on it")
	}

	db.mem = NewMemtable(db.internalComparator)
	db.name = dbname
	db.versions = NewVersionSet(dbname, opt, db.internalComparator)

	db.Lock()
	defer db.Unlock()

	edit, st := db.Recover()
	if st.IsOK() {
		if edit == nil {
			edit = &VersionEdit{} // force LogAndApply
			//edit.SetLogNumber(db.versions.NewFileNumber()) // 3 for new db
			edit.SetLogNumber(db.versions.LogNumber())
		} else {
			debug.Println("recover edit ", edit.LogNumber)
		}
	} else {
		db.Close()
		return nil, st
	}

	db.logNumber = edit.LogNumber
	debug.Println("Open db with log number ", db.logNumber)

	if lfile, st := opt.Env.NewAppendableFile(LogFileName(dbname, db.logNumber)); !st.IsOK() {
		db.Close()
		return nil, st
	} else {
		//TODO 每次重启，log文件都更新。所以需要把recover的log持久化到level0
		db.log = NewLogWriter(lfile)
	}

	if st := db.versions.LogAndApply(edit, &db.Mutex); !st.IsOK() {
		db.Close()
		return nil, st
	} else {
		db.DeleteObsoleteFiles()
	}

	return db, NewStatus(OK)
}

func (db *DB) Close() {
	db.Lock()
	for db.compacting {
		db.bgCV.Wait()
	}

	if db.log != nil {
		db.log.Close()
		db.log = nil
	}
	db.Unlock()

	db.options.Env.UnlockFile(db.lock)
	resetLogger()
}

// Set the database entry for "key" to "value".  Returns OK on success,
// and a non-OK status on error.
// Note: consider setting opt.Sync = true.
func (db *DB) Put(opt *WriteOptions, key, value []byte) Status {
	batch := NewWriteBatch()
	batch.Put(key, value)
	return db.Write(opt, batch)
}

// Remove the database entry (if any) for "key".  Returns OK on
// success, and a non-OK status on error.  It is not an error if "key"
// did not exist in the database.
// Note: consider setting options.sync = true.
func (db *DB) Delete(opt *WriteOptions, key []byte) Status {
	batch := NewWriteBatch()
	batch.Delete(key)
	return db.Write(opt, batch)
}

// If the database contains an entry for "key" store the
// corresponding value in *value and return OK.
//
// If there is no entry for "key" leave *value unchanged and return
// a status for which Status::IsNotFound() returns true.
//
// May return some other Status on an error.
func (db *DB) Get(opt *ReadOptions, key []byte) ([]byte, Status) {
	snapshot := opt.Snapshot

	db.Lock()
	if snapshot > kMaxSequenceNumber {
		snapshot = db.versions.LastSequence()
	}
	mem := db.mem
	imm := db.imm
	db.Unlock()

	lk := NewLookupKey(key, snapshot)
	var value []byte

	succ, status := mem.Get(lk, &value)
	debug.Printf("db.Get with snapshot %v, succ %v, status %v", snapshot, succ, status)
	if succ {
		return value, status
	}

	if imm != nil {
		succ, status = imm.Get(lk, &value)
		if succ {
			return value, status
		}
	}

	// TODO Get data from version

	return nil, status
}

// Information kept for every waiting writer
type dbWriter struct {
	status Status
	batch  *WriteBatch
	sync   bool
	done   bool

	cond *sync.Cond
}

func (db *DB) Write(opt *WriteOptions, myBatch *WriteBatch) Status {
	myWriter := dbWriter{
		batch: myBatch,
		sync:  opt.Sync,
		cond:  sync.NewCond(&db.Mutex),
		done:  false,
	}

	db.Lock()
	defer db.Unlock()

	element := db.writers.PushBack(&myWriter)
	for !myWriter.done && element.Prev() != nil {
		myWriter.cond.Wait()
	}
	if myWriter.done {
		return myWriter.status
	}

	status := db.makeRoomForWrite()
	lastSeq := db.versions.LastSequence()
	lastWriterElem := element

	if status.IsOK() && myBatch != nil {
		mustNotBeSync := !opt.Sync
		updates := db.BuildBatchGroup(&lastWriterElem, mustNotBeSync)

		// incr seq under lock protection
		updates.SetSequence(lastSeq + 1)
		lastSeq += SequenceNumber(updates.Count())

		db.Unlock()

		// write to wal log
		db.log.AddRecord(updates.Contents())
		debug.Println("insert wal log record bytes:", len(updates.Contents()))
		if opt.Sync {
			db.log.Sync()
		}
		// insert into memtable without db lock
		mwbp := memtableWriteBatchProcessor{memtable: db.mem}
		updates.ForEach(&mwbp)

		db.Lock()

		if updates == db.writeBatch {
			db.writeBatch.Clear()
		}
		db.versions.SetLastSequence(lastSeq)
	}

	for {
		ready := db.writers.Front()
		readyWriter := db.writers.Remove(ready).(*dbWriter)
		if readyWriter != &myWriter {
			readyWriter.status = status
			readyWriter.done = true
			readyWriter.cond.Signal()
		}

		if ready == lastWriterElem {
			break
		}
	}

	// Notify new head of write queue
	if db.writers.Len() != 0 {
		// 说明这个front()比我晚入队。
		db.writers.Front().Value.(*dbWriter).cond.Signal()
		/* 唤醒函数开头的这行代码：
		for !myWriter.done && element.Prev() != nil {
			myWriter.cond.Wait()
		}
		*/
	}

	return status
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
func (db *DB) BuildBatchGroup(lastWriter **list.Element, mustNotBeSync bool) *WriteBatch {
	if db.writers.Len() == 0 {
		debug.Panic("why writers are empty")
	}

	first := db.writers.Front()
	firstWriter := first.Value.(*dbWriter)
	result := firstWriter.batch
	size := result.ByteSize()

	// Allow the group to grow up to a maximum size, but if the
	// original write is small, limit the growth so we do not slow
	// down the small write too much.
	// 如果当前的写请求太小(低于128K)，那么总大小也限制低一些，担心拖慢当前写请求。
	maxSize := 1 << 20 // 1MB
	if size <= (128 << 10) {
		maxSize = size + (128 << 10)
	}

	*lastWriter = first
	for e := db.writers.Front().Next(); e != nil; e = e.Next() {
		w := e.Value.(*dbWriter)
		if mustNotBeSync {
			// The first request is not sync, so the batch of requests
			// will not sync. If w is sync, it can't be include in the batch.
			// Or else data may be lost for w.
			if w.sync {
				break
			}
		}

		if w.batch != nil {
			size += w.batch.ByteSize()
			if size > maxSize {
				break
			}

			if result == firstWriter.batch {
				result = db.writeBatch
				if result.Count() != 0 {
					debug.Panicln("Should be zero, wrong db.writeBatch.Count ", result.Count())
				}
				result.Append(firstWriter.batch)
			}

			result.Append(w.batch)
		}

		*lastWriter = e
	}

	return result
}

func (db *DB) makeRoomForWrite() Status {
	if db.writers.Len() == 0 {
		debug.Panicln("makeRoomForWrite but writers empty.")
	}

	st := NewStatus(OK)
	if db.mem == nil {
		db.mem = NewMemtable(db.internalComparator)
	}

	for {
		if db.mem.ApproximateMemoryUsage() < db.options.WriteBufferSize {
			break
		} else if db.imm != nil {
			// We have filled up the current memtable, but the previous
			// one is still being compacted, so we wait.
			debug.Println("Current memtable full; waiting...")
			db.bgCV.Wait()
		} else {
			// Attempt to switch to a new memtable and trigger compaction of old
			// 生成新的log文件号（binlog文件和ldb文件公用这个序号
			debug.Println("Current memtable full; begin compact...")
			newLogNumber := db.versions.NewFileNumber()
			var lfile WritableFile
			lfile, st = db.options.Env.NewWritableFile(LogFileName(db.name, newLogNumber))
			if !st.IsOK() {
				db.versions.ReuseFileNumber(newLogNumber)
				break
			}

			db.logNumber = newLogNumber
			db.log.Close()
			db.log = NewLogWriter(lfile)

			db.imm = db.mem
			db.mem = NewMemtable(db.internalComparator)
			db.maybeScheduleCompaction()
		}
	}

	return st
}

func (db *DB) maybeScheduleCompaction() {
	if db.compacting {
		return
	}

	if !db.bgError.IsOK() {
		return
	}

	if db.imm == nil {
		// TODO && !versions.NeedsCompaction()
		return
	}

	db.compacting = true
	go db.backgroundCall()
}

func (db *DB) backgroundCall() {
	db.Lock()
	defer db.Unlock()

	if !db.compacting {
		debug.Panicln("compacting should be true when enter backgroundCall")
	}

	if db.imm != nil {
		db.CompactMemTable() // minor compacting
	} else {
		// major compaction
	}

	db.compacting = false
	db.maybeScheduleCompaction()

	db.bgCV.Signal()
}

func (db *DB) CompactMemTable() {
	// Save the contents of the memtable as a new Table
	//Version* base = versions_->current();
	s, edit := db.WriteLevel0Table(db.imm) // 将新的ldb文件记录到edit中

	// Replace immutable memtable with the generated Table
	if s.IsOK() {
		// 在MakeRoomForWrite的时候已经刷新了logfile序号
		// 在edit中更新了lognumber，舍弃旧的log，因为已经针对它生成了ldb文件并记录在edit。
		edit.SetLogNumber(db.logNumber) // Earlier logs no longer needed
		// apply edit则是一次原子更新：生成新的ldb文件并删除对应的log文件
		debug.Println("Minor compact: evict lognum ", db.logNumber)
		s = db.versions.LogAndApply(edit, &db.Mutex)
	}

	if s.IsOK() {
		db.imm = nil
		db.DeleteObsoleteFiles()
	} else {
		db.RecordBackgroundError(s)
	}
}

func (db *DB) RecordBackgroundError(err Status) {
	//mutex_.AssertHeld();
	if db.bgError.IsOK() {
		db.bgError = err
		db.bgCV.Broadcast()
	}
}

// 新的ldb文件记录到edit中
func (db *DB) WriteLevel0Table(mem *MemTable) (Status, *VersionEdit) {
	var edit VersionEdit

	var meta FileMetaData
	meta.Number = db.versions.NewFileNumber()

	fname := TableFileName(db.name, meta.Number)
	var builder *TableBuilder
	if wfile, st := db.options.Env.NewWritableFile(fname); !st.IsOK() {
		debug.Println("NewWritableFile failed: ", fname)
		return st, nil
	} else {
		builder = NewTableBuilder(db.options, wfile)
	}
	debug.Printf("Level-0 table #%v: started", meta.Number)

	s := NewStatus(OK)
	{
		db.Unlock()
		// 将内存imm_有序的保存到ldb文件
		s = BuildTable(mem, builder, &meta)
		db.Lock()
	}

	debug.Printf("Level-0 table #%v: %v bytes %v", meta.Number, meta.FileSize, s)

	// Note that if file_size is zero, the file has been deleted and
	// should not be added to the manifest.
	var level int = 0
	if s.IsOK() && meta.FileSize > 0 {
		//minUserKey := meta.Smallest.UserKey();
		//maxUserKey := meta.Largest.UserKey();
		// 测试一下应该加入到当前版本的那一层？
		level = 0
		// 记录新ldb文件的元数据
		edit.AddFile(level, meta.Number, meta.FileSize, meta.Smallest, meta.Largest)
	} else {
		db.options.Env.RemoveFile(fname)
	}

	return s, &edit
}

func (db *DB) Recover() (*VersionEdit, Status) {
	s := NewStatus(OK)
	var edit *VersionEdit

	if db.options.Env.FileExists(CurrentFileName(db.name)) {
		if db.options.ErrorIfExists {
			return nil, NewStatus(InvalidArgument, db.name+" exists (error_if_exists is true)")
		}
	} else {
		if db.options.CreateIfMissing {
			if s = db.initDB(db.options.Env, db.name); !s.IsOK() {
				return nil, s
			}
		} else {
			return nil, NewStatus(InvalidArgument, db.name+" does not exist (create_if_missing is false)")
		}
	}

	// 访问manifest，记录最小有效lognumber，next file号。
	s = db.versions.Recover()

	// 下面db则尝试根据lognumber，恢复有效的wal。leveldb恢复完成后，强制生成了ldb文件
	// 这里我不打算生成ldb，想复用已有的wal文件。当后续接受put请求，再触发minor压缩
	// 所以这里我不需要更新edit,leveldb更新edit，是因为新增了ldb文件，且淘汰相应的wal.log文件
	if s.IsOK() {
		var maxSeq SequenceNumber

		// Recover from all newer log files than the ones named in the
		// descriptor (new log files may have been added by the previous
		// incarnation without registering them in the descriptor).
		minLog := db.versions.LogNumber()
		filenames, st := db.options.Env.GetChildren(db.name)
		if !st.IsOK() {
			return nil, st
		}

		logs := make([]uint64, 0)
		for _, name := range filenames {
			if ok, number, tp := ParseFileName(name); ok {
				if tp == LogFile && number >= minLog {
					logs = append(logs, number)
				}
			}
		}

		sort.Slice(logs, func(i, j int) bool {
			return logs[i] < logs[j]
		})

		for _, n := range logs {
			seq := db.recoverLog(n)
			if maxSeq < seq {
				maxSeq = seq
			}

			// The previous incarnation may not have written any MANIFEST
			// records after allocating this log number.  So we manually
			// update the file number allocation counter in VersionSet.
			db.versions.MarkFileNumberUsed(n)

			if db.mem.ApproximateMemoryUsage() >= db.options.WriteBufferSize {
				debug.Println("after recover log begin compacting")

				if st, ve := db.WriteLevel0Table(db.mem); !st.IsOK() {
					// Reflect errors immediately so that conditions like full
					// file-systems cause the DB::Open() to fail.
					return nil, st
				} else {
					if len(ve.NewFiles) != 1 {
						debug.Panicln("WriteLevel0Table ldb file should be 1 but ", len(ve.NewFiles))
					}

					if edit == nil {
						edit = ve
					} else {
						f := ve.NewFiles[0]
						edit.AddFile(f.Level, f.Number, f.FileSize, f.Smallest, f.Largest)
					}

					edit.LogNumber = n + 1 // old log number n is not needed
					db.mem = nil
					debug.Println("WriteLevel0Table evict log number ", n)
				}
			}
		}

		if maxSeq > db.versions.LastSequence() {
			db.versions.SetLastSequence(maxSeq)
		}
	}

	return edit, s
}

func (db *DB) recoverLog(num uint64) SequenceNumber {
	var maxSeq SequenceNumber
	if logf, st := db.options.Env.NewSequentialFile(LogFileName(db.name, num)); st.IsOK() {
		if db.mem == nil {
			db.mem = NewMemtable(db.internalComparator)
		}

		reader := NewLogReader(logf)
		var record []byte
		for reader.ReadRecord(&record) {
			wb := NewWriteBatch()
			wb.SetContents(record)
			debug.Printf("read log record len: %v, write count %v", len(record), wb.Count())
			if wb.Count() == 0 {
				debug.Panicln("Why write batch count is 0")
			}

			// insert into memtable without db lock
			mwbp := memtableWriteBatchProcessor{memtable: db.mem}
			wb.ForEach(&mwbp)

			// calc max seq
			endSeq := wb.Sequence() + SequenceNumber(wb.Count()-1)
			if maxSeq < endSeq {
				maxSeq = endSeq
			}
		}
		debug.Println("recover logfile done:", LogFileName(db.name, num))
		reader.Close()
	}

	return maxSeq
}

func (db *DB) initDB(env Env, dbname string) Status {
	s := NewStatus(OK)

	var edit VersionEdit
	edit.SetComparatorName(db.internalComparator.userCmp.Name()) // leveldb.InternalKeyComparator
	edit.SetLogNumber(0)
	edit.SetNextFile(2) // The first file is MANIFEST-1
	edit.SetLastSequence(0)

	// manifest = dbname/MANIFEST-1
	var wfile WritableFile
	manifest := DescriptorFileName(db.name, 1)
	if wfile, s = db.options.Env.NewWritableFile(manifest); !s.IsOK() {
		return s
	}

	// write edit to MANIFEST-1
	logWriter := NewLogWriter(wfile)
	s = logWriter.AddRecord(edit.Encode())
	if s.IsOK() {
		s = logWriter.Close()
	}

	if s.IsOK() {
		// Make "CURRENT" file that points to the new manifest file.
		if err := SetCurrentFile(db.name, 1); err != nil {
			s = NewStatus(IOError, err.Error())
		} else {
			debug.Println("initDB: SetCurrentFile to ", manifest)
		}
	}

	if !s.IsOK() {
		db.options.Env.RemoveFile(manifest)
	}

	return s
}

func (db *DB) DeleteObsoleteFiles() {
	if !db.bgError.IsOK() {
		// After a background error, we don't know whether a new version may
		// or may not have been committed, so we cannot safely garbage collect.
		return
	}

	// Make a set of all of the live ldb files
	live := db.versions.LiveFiles()

	minLog := db.versions.LogNumber()
	filenames, _ := db.options.Env.GetChildren(db.name) // Ignoring errors on purpose
	for _, name := range filenames {
		if ok, number, tp := ParseFileName(name); ok {
			keep := true
			switch tp {
			case LogFile:
				debug.Printf("logfile: my number %v, min %v", number, minLog)
				if number < minLog {
					keep = false
				}
			case DescriptorFile:
				// Keep my manifest file, and any newer incarnations'
				// (in case there is a race that allows other incarnations)
				debug.Printf("manifest: my number %v, ver number %v", number, db.versions.ManifestFileNumber())
				if number < db.versions.ManifestFileNumber() {
					keep = false
				}
			case TableFile:
				i := sort.Search(len(live), func(i int) bool {
					return number <= live[i]
				})

				if i < len(live) && live[i] == number {
					keep = true
				} else {
					keep = false
				}

				debug.Printf("keep %v: %v.ldb", keep, number)

			case TempFile:
				//TODO
			case CurrentFile:
				fallthrough
			case DBLockFile:
				fallthrough
			case InfoLogFile:
				keep = true
			}

			if !keep {
				debug.Printf("Delete type=%v #%v", tp, number)
				db.options.Env.RemoveFile(db.name + "/" + name)
			}
		}
	}
}

type memtableWriteBatchProcessor struct {
	memtable *MemTable
}

func (wbp *memtableWriteBatchProcessor) ProcessWriteBatch(seq SequenceNumber, tp ValueType, key, value []byte) {
	debug.Printf("ProcessWriteBatch key %v, value %v, seq %v", string(key), string(value), seq)
	wbp.memtable.Add(seq, tp, key, value)
}
