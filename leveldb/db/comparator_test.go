package leveldb

// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

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

var comparator Comparator

func setup() {
	log.Println("setup...")
	comparator = NewBytewiseComparator()
}

func teardown() {
	log.Println("teardown...")
}

func TestFindShortestSeparator(t *testing.T) {
	var tests = []struct {
		start []byte
		limit string
		want  string
	}{
		{[]byte("shandong"), "shanghai", "shane"},
		{[]byte("shang"), "shanghai", "shang"},
		{[]byte("shanghai"), "shenzhen", "shb"},
	}

	for _, v := range tests {
		comparator.FindShortestSeparator(&v.start, v.limit)
		got := v.start
		if string(got) != v.want {
			t.Errorf("FindShortestSeparator got %v but want %v", got, v.want)
		}
	}
}

func TestFindShortSuccessor(t *testing.T) {
	var tests = []struct {
		key  []byte
		want []byte
	}{
		{[]byte("shanghai"), []byte("t")},
		{[]byte("shenzhen"), []byte("t")},
		{[]byte("\xFF"), []byte("\xFF")},
		{[]byte("\xFFb"), []byte("\xFFc")},
	}

	for _, v := range tests {
		comparator.FindShortSuccessor(&v.key)
		got := v.key
		if string(got) != string(v.want) {
			t.Errorf("FindShortSuccessor got %v but want %v", got, v.want)
		}
	}
}
