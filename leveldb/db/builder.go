// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Build a Table file from the contents of *iter.  The generated file
// will be named according to meta->number.  On success, the rest of
// *meta will be filled with metadata about the generated table.
// If no data is present in *iter, meta->file_size will be set to
// zero, and no Table file will be produced.

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

package leveldb

import ()

// No lock for this func call
func BuildTable(mem *MemTable, builder *TableBuilder, meta *FileMetaData) Status {
	meta.FileSize = 0

	iter := NewMemTableIterator(mem)
	iter.SeekToFirst()
	s := iter.Status()
	// Check for input iterator errors
	if !s.IsOK() {
		return s
	}

	meta.Smallest.DecodeFrom(iter.Key()) // 记录最小的key
	var largestKey []byte
	for ; iter.Valid(); iter.Next() {
		largestKey = iter.Key()
		builder.Add(iter.Key(), iter.Value())
	}
	meta.Largest.DecodeFrom(largestKey)

	// Finish and check for builder errors
	s = builder.Finish()
	if s.IsOK() {
		meta.FileSize = builder.FileSize()
		if meta.FileSize == 0 {
			debug.Println("ldb size is 0")
		}
	}

	return s
}
