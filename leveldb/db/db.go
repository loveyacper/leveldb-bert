package leveldb

import (
	"container/list"
	"os"
	"sync"
)

type DB struct {
	sync.Mutex

	options            *Options
	mem                *MemTable
	internalComparator *InternalKeyComparator

	writers    *list.List
	sequence   SequenceNumber
	writeBatch *WriteBatch
}

func NewDB(opt *Options) *DB {
	if opt.Env == nil {
		opt.Env = DefaultEnv()
	}

	db := &DB{}
	db.options = opt
	db.internalComparator = &InternalKeyComparator{opt.Comp}
	db.writers = list.New()
	db.writeBatch = NewWriteBatch()

	return db
}

func Open(opt *Options, dbname string) (*DB, Status) {
	db := NewDB(opt)
	opt.Env.CreateDir(dbname)

	// If the file doesn't exist, create it, or append to the file
	if f, err := os.OpenFile(dbname+"/LOG", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
		initLogger(f)
	} else {
		initLogger(os.Stdout)
	}
	debug.Println("open db ", dbname)

	db.Lock()
	defer db.Unlock()

	db.mem = NewMemtable(db.internalComparator)
	// TODO recover, read wal

	return db, NewStatus(OK)
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
		snapshot = db.sequence
	}
	db.Unlock()

	lk := NewLookupKey(key, snapshot)
	var value []byte
	succ, status := db.mem.Get(lk, &value)

	if succ && status.IsOK() {
		return value, status
	} else {
		return nil, status
	}
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

	status := NewStatus(OK)
	lastSeq := db.sequence
	lastWriterElem := element

	if status.IsOK() && myBatch != nil {
		mustNotBeSync := !opt.Sync
		updates := db.BuildBatchGroup(&lastWriterElem, mustNotBeSync)

		// incr seq under lock protection
		updates.SetSequence(lastSeq + 1)
		lastSeq += SequenceNumber(updates.Count())

		db.Unlock()
		// TODO write to wal log
		if opt.Sync {
			// sync to wal log
		}
		// insert into memtable without db lock
		mwbp := memtableWriteBatchProcessor{memtable: db.mem}
		updates.ForEach(&mwbp)

		db.Lock()

		if updates == db.writeBatch {
			db.writeBatch.Clear()
		}
		db.sequence = lastSeq
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

type memtableWriteBatchProcessor struct {
	memtable *MemTable
}

func (wbp *memtableWriteBatchProcessor) ProcessWriteBatch(seq SequenceNumber, tp ValueType, key, value []byte) {
	wbp.memtable.Add(seq, tp, key, value)
}
