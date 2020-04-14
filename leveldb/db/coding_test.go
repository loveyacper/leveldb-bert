// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"testing"
)

func TestFixed(t *testing.T) {
	dst := bytes.NewBuffer(nil)
	var u32 uint32 = 0x12345678
	PutFixed32(dst, 0x12345678)
	if v32, err := DecodeFixed32(dst); err != nil || v32 != u32 {
		t.Errorf("Fixed32 expect %v but %v, error %v", u32, v32, err)
	}

	dst.Reset()
	var u64 uint64 = 0x1234567890
	PutFixed64(dst, u64)
	if v64, err := DecodeFixed64(dst); err != nil || v64 != u64 {
		t.Errorf("Fixed64 expect %v but %v, error %v", u64, v64, err)
	}
}

func TestLengthPrefixedSlice(t *testing.T) {
	dst := bytes.NewBuffer(nil)
	value := []byte("helloworld")
	PutLengthPrefixedSlice(dst, value)
	res := make([]byte, len(value))
	if err := GetLengthPrefixedSlice(dst, res); err != nil {
		t.Errorf("GetLengthPrefixedSlice error %v", err)
	}
}

func TestVarint(t *testing.T) {
	dst := bytes.NewBuffer(nil)
	var u32 uint32 = 0x12345678
	PutVarint32(dst, u32)
	if v32, err := GetVarint32(dst); err != nil || v32 != u32 {
		t.Errorf("Varint32 expect %v but %v, error %v", u32, v32, err)
	}

	dst.Reset()
	var u64 uint64 = 0x1234567890
	PutVarint64(dst, u64)
	if v64, err := GetVarint64(dst); err != nil || v64 != u64 {
		t.Errorf("Varint64 expect %v but %v, error %v", u64, v64, err)
	}
}
