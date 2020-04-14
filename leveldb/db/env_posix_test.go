// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"log"
	"os"
	"sync"
	"testing"
)

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()

	os.Exit(code)
}

var fname string = "file.test"
var env Env

func setup() {
	log.Println("setup...")
	env = DefaultEnv()
}

func teardown() {
	log.Println("teardown...")
	env.RemoveFile(fname)
}

func TestPosixWritableFile(t *testing.T) {
	file, st := env.NewWritableFile(fname)
	if !st.IsOK() {
		t.Errorf("TestPosixWritableFile new failed")
	}

	defer file.Close()

	file.Append([]byte("hello"))
	file.Append([]byte(",world"))
}

func TestPosixSequentialFile(t *testing.T) {
	file, st := env.NewSequentialFile(fname)
	if !st.IsOK() {
		t.Errorf("TestPosixSequentialFile new failed")
	}

	defer file.Close()

	scratch := make([]byte, 8)
	d, st := file.Read(len(scratch), scratch)
	if !st.IsOK() || len(d) != len(scratch) {
		t.Errorf("TestPosixSequentialFile failed")
	}
}

func TestPosixRandomAccessFile(t *testing.T) {
	file, st := env.NewRandomAccessFile(fname)
	if !st.IsOK() {
		t.Errorf("TestPosixRandomAccessFile new failed")
	}

	defer file.Close()

	scratch := make([]byte, 8)
	// read "world" at offset 6
	expect := "world"
	d, st := file.Read(6, len(scratch), scratch)
	if !st.IsOK() || string(d) != expect {
		t.Errorf("TestPosixRandomAccessFile failed %v", st)
	}
}

func TestPosixLockFile(t *testing.T) {
	lock, st := env.LockFile(fname)
	if !st.IsOK() {
		t.Errorf("TestPosixLockFile new failed")
	}

	defer lock.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, st := env.LockFile(fname)
		if st.IsOK() {
			t.Errorf("TestPosixLockFile failed")
		} else {
			t.Logf("TestPosixLockFile failed because %v", st)
		}
	}()
	wg.Wait()

	env.UnlockFile(lock)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, st := env.LockFile(fname)
		if st.IsOK() {
			t.Logf("TestPosixLockFile success")
		} else {
			t.Errorf("TestPosixLockFile failed %v", st)
		}
	}()
	wg.Wait()
}
