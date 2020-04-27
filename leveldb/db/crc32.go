// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"hash/crc32"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type levelDbCrc uint32

// Update returns the result of adding the bytes in p to the crc.
func (crc levelDbCrc) Extend(p []byte) levelDbCrc {
	return levelDbCrc(crc32.Update(uint32(crc), crcTable, p))
}

// Return the crc32c of data[0,n-1]
func CrcValue(p []byte) levelDbCrc {
	var crc levelDbCrc = 0
	return crc.Extend(p)
}

// Value returns a masked crc.
func (crc levelDbCrc) Mask() levelDbCrc {
	c := uint32(crc)
	return levelDbCrc((c>>15 | c<<17) + 0xa282ead8)
}
