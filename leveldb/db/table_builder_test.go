package leveldb

// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()

	os.Exit(code)
}

var table *TableBuilder

func setup() {
	initLogger(os.Stdout)

	env := DefaultEnv()
	file, _ := env.NewWritableFile("bert.ldb")
	table = NewTableBuilder(NewOptions(), file)
}

func teardown() {
}

func TestTableBuilder(t *testing.T) {
	table.Add([]byte("city"), []byte("shenzhen"))
	table.Add([]byte("height"), []byte("160"))
	table.Finish()
}
