// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

// A Status encapsulates the result of an operation. It may indicate success,
// or it may indicate an error with an associated error message.

import (
	"fmt"
	"strings"
)

const (
	OK byte = iota
	NotFound
	Corruption
	NotSupported
	InvalidArgument
	IOError
)

type Status interface {
	fmt.Stringer

	// !!! nil status is OK
	IsOK() bool
	IsNotFound() bool
	IsCorruption() bool
	IsNotSupported() bool
	IsInvalidArgument() bool
	IsIOError() bool

	// builtin error interface
	error
}

type LevelDBStatus struct {
	state byte
	msg   string
}

func NewStatus(s byte, msg ...string) Status {
	if s == OK {
		var ok *LevelDBStatus
		return ok
	}

	return &LevelDBStatus{state: s, msg: strings.Join(msg, ",")}
}

func (s *LevelDBStatus) IsOK() bool {
	if s == nil {
		return true
	}

	return s.state == OK
}

func (s *LevelDBStatus) IsNotFound() bool {
	return s.state == NotFound
}

func (s *LevelDBStatus) IsCorruption() bool {
	return s.state == Corruption
}

func (s *LevelDBStatus) IsNotSupported() bool {
	return s.state == NotSupported
}

func (s *LevelDBStatus) IsInvalidArgument() bool {
	return s.state == InvalidArgument
}

func (s *LevelDBStatus) IsIOError() bool {
	return s.state == IOError
}

// Return a string representation of this status suitable for printing.
// Returns the string "OK" for success.
func (s *LevelDBStatus) String() string {
	if s.IsOK() {
		return "OK"
	}

	var strs []string
	switch s.state {
	case OK:
		strs = append(strs, "OK")
	case NotFound:
		strs = append(strs, "NotFound")
	case Corruption:
		strs = append(strs, "Corruption")
	case NotSupported:
		strs = append(strs, "Not implemented")
	case InvalidArgument:
		strs = append(strs, "Invalid argument")
	case IOError:
		strs = append(strs, "IO error")
	default:
		strs = append(strs, "Unknown error")
	}

	if len(s.msg) > 0 {
		strs = append(strs, s.msg)
	}

	return strings.Join(strs, " : ")
}

func (s *LevelDBStatus) Error() string {
	return s.String()
}
