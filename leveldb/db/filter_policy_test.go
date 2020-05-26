package leveldb

// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

import (
	"bytes"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()

	os.Exit(code)
}

var bloom FilterPolicy
var filter []byte
var keys [][]byte

func setup() {
	bloom = NewBloomFilterPolicy(10)
}

func teardown() {
}

func TestEmptyFilter(t *testing.T) {
	if bloom.KeyMayMatch([]byte("Hello"), filter) {
		t.Errorf("Empty filter should NOT match Hello")
	}
}

func TestSmallFilter(t *testing.T) {
	keys = make([][]byte, 0)
	keys = append(keys, []byte("hello"))
	keys = append(keys, []byte("world"))

	filter = make([]byte, 0)
	bloom.CreateFilter(keys, &filter)
	keys = nil

	if !bloom.KeyMayMatch([]byte("hello"), filter) {
		t.Errorf("Small filter should match hello")
	}
	if !bloom.KeyMayMatch([]byte("world"), filter) {
		t.Errorf("Small filter should match world")
	}
	if bloom.KeyMayMatch([]byte("xx"), filter) {
		t.Errorf("Small filter should NOT match xx")
	}
	if bloom.KeyMayMatch([]byte("foo"), filter) {
		t.Errorf("Small filter should NOT match foo")
	}
}

func TestBigFilter(t *testing.T) {
	keyNum := 10000
	keys = make([][]byte, 0)
	filter = make([]byte, 0)

	for i := 0; i < keyNum; i++ {
		buf := bytes.NewBuffer(nil)
		PutFixed32(buf, uint32(i))
		keys = append(keys, buf.Bytes())
	}

	bloom.CreateFilter(keys, &filter)
	keys = nil

	buf := bytes.NewBuffer(nil)
	for i := 0; i < keyNum; i++ {
		PutFixed32(buf, uint32(i))
		if !bloom.KeyMayMatch(buf.Bytes(), filter) {
			t.Errorf("Big filter should match %v", i)
		}

		buf.Reset()
	}

	// check false positive
	falseCount := 0
	for i := 0; i < keyNum; i++ {
		PutFixed32(buf, uint32(i+10000000))
		if bloom.KeyMayMatch(buf.Bytes(), filter) {
			falseCount++
		}
		buf.Reset()
	}

	falseRate := float64(falseCount) / float64(keyNum)
	falseRate *= 100
	t.Logf("False rate %v%%", falseRate)
	if falseRate > 2 {
		t.Errorf("False rate too big %v%%", falseRate)
	}
}
