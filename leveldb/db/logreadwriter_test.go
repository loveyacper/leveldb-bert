// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"log"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()

	os.Exit(code)
}

var writer *LogWriter
var reader *LogReader
var env Env
var filename string = "test.log"

func setup() {
	log.Println("setup...")
	env = DefaultEnv()
	wf, _ := env.NewWritableFile(filename)
	writer = NewLogWriter(wf)
	rf, _ := env.NewSequentialFile(filename)
	reader = NewLogReader(rf)
}

func teardown() {
	log.Println("teardown...")
	writer.Close()
	reader.Close()
	env.RemoveFile(filename)
}

/*
A: length 1000
B: length 97270
C: length 8000
A will be stored as a FULL record in the first block.

B will be split into three fragments: first fragment occupies the rest of the first block, second fragment occupies the entirety of the second block, and the third fragment occupies a prefix of the third block. This will leave six bytes free in the third block, which will be left empty as the trailer.

C will be stored as a FULL record in the fourth block.
*/
func TestFromLogFormat(t *testing.T) {
	writer.SetCrash(false)

	a := make([]byte, 1000)
	a[0] = 'a'
	a[999] = 'a'
	crca := CrcValue(a)
	writer.AddRecord(a)

	b := make([]byte, 97270)
	b[0] = 'b'
	b[97269] = 'b'
	crcb := CrcValue(b)
	writer.AddRecord(b)

	c := make([]byte, 8000)
	c[0] = 'c'
	c[7999] = 'c'
	crcc := CrcValue(c)
	writer.AddRecord(c)

	var r []byte
	s1 := reader.ReadRecord(&r)
	t.Logf("read a %v, len %d\n", s1, len(r))
	if !s1 || crca != CrcValue(r) {
		t.Errorf("read a %v, crc %v vs %v", s1, crca, CrcValue(r))
	}
	s1 = reader.ReadRecord(&r)
	t.Logf("read b %v, len %d\n", s1, len(r))
	if !s1 || crcb != CrcValue(r) {
		t.Errorf("read b %v, crc %v vs %v", s1, crcb, CrcValue(r))
	}
	s1 = reader.ReadRecord(&r)
	t.Logf("read c %v, len %d\n", s1, len(r))
	if !s1 || crcc != CrcValue(r) {
		t.Errorf("read c %v, crc %v vs %v", s1, crcc, CrcValue(r))
	}
	s1 = reader.ReadRecord(&r)
	t.Logf("read not exist %v\n", s1)
}

func TestSmallRecord(t *testing.T) {
	teardown()
	setup()

	writer.SetCrash(false)

	a := make([]byte, 27)
	a[0] = 'a'
	a[26] = 'a'
	crca := CrcValue(a)
	writer.AddRecord(a)

	b := make([]byte, 27)
	b[0] = 'b'
	b[26] = 'b'
	crcb := CrcValue(b)
	writer.AddRecord(b)

	c := make([]byte, 18)
	c[0] = 'c'
	c[17] = 'c'
	crcc := CrcValue(c)
	writer.AddRecord(c)

	var r []byte
	s1 := reader.ReadRecord(&r)
	t.Logf("read a %v, len %d\n", s1, len(r))
	if !s1 || crca != CrcValue(r) {
		t.Errorf("read a %v, crc %v vs %v", s1, crca, CrcValue(r))
	}
	s1 = reader.ReadRecord(&r)
	t.Logf("read b %v, len %d\n", s1, len(r))
	if !s1 || crcb != CrcValue(r) {
		t.Errorf("read b %v, crc %v vs %v", s1, crcb, CrcValue(r))
	}
	s1 = reader.ReadRecord(&r)
	t.Logf("read c %v, len %d\n", s1, len(r))
	if !s1 || crcc != CrcValue(r) {
		t.Errorf("read c %v, crc %v vs %v", s1, crcc, CrcValue(r))
	}
	s1 = reader.ReadRecord(&r)
	t.Logf("read not exist %v\n", s1)
}
