// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bufio"
	"io"
	"os"
	"sync"
	"syscall"
)

type PosixEnv struct {
	lockTable *posixLockTable
}

func NewPosixEnv() *PosixEnv {
	env := &PosixEnv{}
	env.lockTable = newPosixLockTable()
	return env
}

func (env *PosixEnv) NewSequentialFile(fname string) (SequentialFile, Status) {
	f, err := NewPosixSequentialFile(fname)
	if err != nil {
		return nil, NewStatus(IOError, err.Error())
	}

	return f, NewStatus(OK)
}

func (env *PosixEnv) NewRandomAccessFile(fname string) (RandomAccessFile, Status) {
	f, err := NewPosixRandomAccessFile(fname)
	if err != nil {
		return nil, NewStatus(IOError, err.Error())
	}

	return f, NewStatus(OK)
}

func (env *PosixEnv) NewWritableFile(fname string) (WritableFile, Status) {
	f, err := NewPosixWritableFile(fname, false)
	if err != nil {
		return nil, NewStatus(IOError, err.Error())
	}

	return f, NewStatus(OK)
}

func (env *PosixEnv) NewAppendableFile(fname string) (WritableFile, Status) {
	f, err := NewPosixWritableFile(fname, true)
	if err != nil {
		return nil, NewStatus(IOError, err.Error())
	}

	return f, NewStatus(OK)
}

func (env *PosixEnv) FileExists(fname string) bool {
	if err := syscall.Access(fname, syscall.F_OK); err != nil {
		return false
	}

	return true
}

func (env *PosixEnv) GetChildren(dir string) ([]string, Status) {
	dirFile, err := os.Open(dir)
	if err != nil {
		return nil, NewStatus(IOError, err.Error())
	}

	names, err := dirFile.Readdirnames(-1)
	dirFile.Close()
	if err != nil {
		return nil, NewStatus(IOError, err.Error())
	}

	return names, NewStatus(OK)
}

func (env *PosixEnv) RemoveFile(fname string) Status {
	err := os.Remove(fname)
	if err != nil {
		return NewStatus(IOError, err.Error())
	}

	return NewStatus(OK)
}

func (env *PosixEnv) CreateDir(dir string) Status {
	err := os.Mkdir(dir, 0755)
	if err != nil {
		return NewStatus(IOError, err.Error())
	}

	return NewStatus(OK)
}

func (env *PosixEnv) RemoveDir(dir string) Status {
	err := os.RemoveAll(dir)
	if err != nil {
		return NewStatus(IOError, err.Error())
	}

	return NewStatus(OK)
}

func (env *PosixEnv) GetFileSize(fname string) (int64, Status) {
	info, err := os.Stat(fname)
	if err != nil {
		return 0, NewStatus(IOError, err.Error())
	}

	return info.Size(), NewStatus(OK)
}

func (env *PosixEnv) RenameFile(src, target string) Status {
	err := os.Rename(src, target)
	if err != nil {
		return NewStatus(IOError, err.Error())
	}

	return NewStatus(OK)
}

func (env *PosixEnv) LockFile(fname string) (FileLock, Status) {
	fl, err := NewPosixFileLock(fname)
	if err != nil {
		return nil, NewStatus(IOError, err.Error())
	}

	if !env.lockTable.insert(fname) {
		fl.Close()
		return nil, NewStatus(IOError, fname+" already be locked")
	}

	if st := fl.Flock(); !st.IsOK() {
		fl.Close()
		env.lockTable.remove(fname)
		return nil, st
	}

	return fl, NewStatus(OK)
}

func (env *PosixEnv) UnlockFile(lock FileLock) Status {
	posixLock := lock.(*PosixFileLock)
	if st := posixLock.Funlock(); !st.IsOK() {
		return st
	}

	env.lockTable.remove(posixLock.Name())
	return NewStatus(OK)
}

// Implements sequential read access in a file using read().
type PosixSequentialFile struct {
	*os.File
}

func NewPosixSequentialFile(fname string) (*PosixSequentialFile, error) {
	file := &PosixSequentialFile{}
	if f, err := os.Open(fname); err != nil {
		return nil, err
	} else {
		file.File = f
	}

	return file, nil
}

func (file *PosixSequentialFile) Read(n int, scratch []byte) ([]byte, Status) {
	if n > len(scratch) {
		return nil, NewStatus(InvalidArgument, "Buffer too small")
	}

	if l, err := file.File.Read(scratch[:n]); err != nil {
		return nil, NewStatus(IOError, err.Error())
	} else {
		return scratch[:l], NewStatus(OK)
	}
}

func (file *PosixSequentialFile) Skip(n int64) Status {
	if _, err := file.Seek(n, os.SEEK_CUR); err != nil {
		return NewStatus(IOError, err.Error())
	}
	return NewStatus(OK)
}

// Implements random read access in a file using pread().
type PosixRandomAccessFile struct {
	*os.File
}

func NewPosixRandomAccessFile(fname string) (*PosixRandomAccessFile, error) {
	file := &PosixRandomAccessFile{}
	if f, err := os.Open(fname); err != nil {
		return nil, err
	} else {
		file.File = f
	}

	return file, nil
}

func (file *PosixRandomAccessFile) Read(offset int64, n int, scratch []byte) ([]byte, Status) {
	if n > len(scratch) {
		return nil, NewStatus(InvalidArgument, "Buffer too small")
	}

	l, err := file.File.ReadAt(scratch[:n], offset)
	if err != nil && err != io.EOF {
		return nil, NewStatus(IOError, err.Error())
	} else {
		return scratch[:l], NewStatus(OK)
	}
}

type PosixWritableFile struct {
	*os.File
	*bufio.Writer
}

func NewPosixWritableFile(fname string, appendable bool) (*PosixWritableFile, error) {
	flag := 0
	var perm os.FileMode = 0666

	if appendable {
		flag = (os.O_CREATE | os.O_WRONLY | os.O_APPEND)
	} else {
		flag = (os.O_CREATE | os.O_WRONLY | os.O_TRUNC)
	}

	file := &PosixWritableFile{}
	if f, err := os.OpenFile(fname, flag, perm); err != nil {
		return nil, err
	} else {
		file.File = f
		file.Writer = bufio.NewWriter(f)
	}

	return file, nil
}

func (file *PosixWritableFile) Close() error {
	st := file.Sync()
	if err := file.File.Close(); err != nil {
		if st.IsOK() {
			st = NewStatus(IOError, err.Error())
		}
	}

	if st.IsOK() {
		st = nil
	}

	return st
}

func (file *PosixWritableFile) Flush() Status {
	if err := file.Writer.Flush(); err != nil {
		return NewStatus(IOError, err.Error())
	}

	return NewStatus(OK)
}

func (file *PosixWritableFile) Sync() Status {
	if st := file.Flush(); !st.IsOK() {
		return st
	}

	if err := file.File.Sync(); err != nil {
		return NewStatus(IOError, err.Error())
	}

	return NewStatus(OK)
}

func (file *PosixWritableFile) Append(data []byte) Status {
	if _, err := file.Writer.Write(data); err != nil {
		return NewStatus(IOError, err.Error())
	}

	return NewStatus(OK)
}

type PosixFileLock struct {
	*os.File
}

func NewPosixFileLock(fname string) (*PosixFileLock, error) {
	file := &PosixFileLock{}
	if f, err := os.Create(fname); err != nil {
		return nil, err
	} else {
		file.File = f
	}

	return file, nil
}

func (file *PosixFileLock) Flock() Status {
	flockT := syscall.Flock_t{
		Type:   syscall.F_WRLCK,
		Whence: io.SeekStart,
		Start:  0,
		Len:    0,
	}

	if err := syscall.FcntlFlock(file.Fd(), syscall.F_SETLK, &flockT); err != nil {
		return NewStatus(IOError, err.Error())
	}

	return NewStatus(OK)
}

func (file *PosixFileLock) Funlock() Status {
	flockT := syscall.Flock_t{
		Type:   syscall.F_UNLCK,
		Whence: io.SeekStart,
		Start:  0,
		Len:    0,
	}

	if err := syscall.FcntlFlock(file.Fd(), syscall.F_SETLK, &flockT); err != nil {
		return NewStatus(IOError, err.Error())
	}

	return NewStatus(OK)
}

// Tracks the files locked by PosixEnv::LockFile().
//
// We maintain a separate set instead of relying on fcntl(F_SETLK) because
// fcntl(F_SETLK) does not provide any protection against multiple uses from the
// same process.
//
// Instances are thread-safe because all member data is guarded by a mutex.
type posixLockTable struct {
	sync.Mutex
	lockedFiles map[string]struct{}
}

func newPosixLockTable() *posixLockTable {
	tbl := &posixLockTable{}
	tbl.lockedFiles = make(map[string]struct{})
	return tbl
}

func (tbl *posixLockTable) insert(fname string) bool {
	succ := false

	tbl.Lock()
	if _, ok := tbl.lockedFiles[fname]; !ok {
		succ = true
		tbl.lockedFiles[fname] = struct{}{}
	}
	tbl.Unlock()

	return succ
}

func (tbl *posixLockTable) remove(fname string) {
	tbl.Lock()
	delete(tbl.lockedFiles, fname)
	tbl.Unlock()
}
