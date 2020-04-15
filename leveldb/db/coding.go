// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"unsafe"
)

var nativeEndian binary.ByteOrder

func init() {
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

	switch buf {
	case [2]byte{0xCD, 0xAB}:
		nativeEndian = binary.LittleEndian
	case [2]byte{0xAB, 0xCD}:
		nativeEndian = binary.BigEndian
	default:
		panic("Could not determine native endianness.")
	}
}

func PutFixed32(dst *bytes.Buffer, value uint32) {
	var b [4]byte
	nativeEndian.PutUint32(b[:], value)
	dst.Write(b[:])
}

func PutFixed64(dst *bytes.Buffer, value uint64) {
	var b [8]byte
	nativeEndian.PutUint64(b[:], value)
	dst.Write(b[:])
}

func DecodeFixed32(buf *bytes.Buffer) (uint32, error) {
	var b [4]byte
	if _, err := buf.Read(b[:]); err != nil {
		return 0, err
	}

	return nativeEndian.Uint32(b[:]), nil
}

func DecodeFixed64(buf *bytes.Buffer) (uint64, error) {
	var b [8]byte
	if _, err := buf.Read(b[:]); err != nil {
		return 0, err
	}

	return nativeEndian.Uint64(b[:]), nil
}

func PutLengthPrefixedSlice(dst *bytes.Buffer, value []byte) {
	var l uint32 = uint32(len(value))
	PutVarint32(dst, l)
	dst.Write(value)
}

func GetLengthPrefixedSlice(input *bytes.Buffer, result []byte) error {
	if l, err := GetVarint32(input); err != nil {
		return err
	} else {
		_, err := input.Read(result[:l])
		return err
	}
}

func PutVarint32(dst *bytes.Buffer, value uint32) {
	var buf [binary.MaxVarintLen32]byte
	v64 := uint64(value)
	nwritten := binary.PutUvarint(buf[:], v64)
	dst.Write(buf[:nwritten])
}

func PutVarint64(dst *bytes.Buffer, value uint64) {
	var buf [binary.MaxVarintLen64]byte
	nwritten := binary.PutUvarint(buf[:], value)
	dst.Write(buf[:nwritten])
}

func GetVarint32(input *bytes.Buffer) (uint32, error) {
	value, nread := binary.Uvarint(input.Bytes()[:binary.MaxVarintLen32])
	if nread > 0 {
		input.Next(nread)
		return uint32(value), nil
	} else if nread == 0 {
		return 0, io.EOF
	}

	return 0, errors.New("binary: varint overflows a 32-bit integer")
}

func GetVarint64(input *bytes.Buffer) (uint64, error) {
	value, nread := binary.Uvarint(input.Bytes()[:binary.MaxVarintLen64])
	if nread > 0 {
		input.Next(nread)
		return value, nil
	} else if nread == 0 {
		return 0, io.EOF
	}

	return 0, errors.New("binary: varint overflows a 32-bit integer")
}
