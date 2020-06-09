// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type FileType int

const (
	LogFile FileType = iota
	DBLockFile
	TableFile
	DescriptorFile
	CurrentFile
	TempFile
	InfoLogFile

	InvalidFile = 1000
)

// Return the name of the log file with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
func LogFileName(dbname string, number uint64) string {
	//if number < 0 {
	//	debug.Panic("log number should > 0")
	//	}
	return makeFileName(dbname, number, "log")
}

// Return the name of the sstable with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
func TableFileName(dbname string, number uint64) string {
	if number <= 0 {
		debug.Panic("table number should > 0")
	}
	return makeFileName(dbname, number, "ldb")
}

// Return the name of the descriptor file for the db named by
// "dbname" and the specified incarnation number.  The result will be
// prefixed with "dbname".
func DescriptorFileName(dbname string, number uint64) string {
	if number <= 0 {
		debug.Panic("descriptor number should > 0")
	}

	return dbname + fmt.Sprintf("/MANIFEST-%06v", number)
}

// Return the name of the current file.  This file contains the name
// of the current manifest file.  The result will be prefixed with
// "dbname".
func CurrentFileName(dbname string) string {
	return dbname + "/CURRENT"
}

// Return the name of the lock file for the db named by
// "dbname".  The result will be prefixed with "dbname".
func LockFileName(dbname string) string {
	return dbname + "/LOCK"
}

// Return the name of a temporary file owned by the db named "dbname".
// The result will be prefixed with "dbname".
func TempFileName(dbname string, number uint64) string {
	if number <= 0 {
		debug.Panic("tmpfile number should > 0")
	}
	return makeFileName(dbname, number, "dbtmp")
}

// Return the name of the info log file for "dbname".
func InfoLogFileName(dbname string) string {
	return dbname + "/LOG"
}

// Owned filenames have the form:
//    dbname/CURRENT
//    dbname/LOCK
//    dbname/LOG
//    dbname/LOG.old
//    dbname/MANIFEST-[0-9]+
//    dbname/[0-9]+.(log|ldb)
// If filename is a leveldb file, store the type of the file in *type.
// The number encoded in the filename is stored in *number.  If the
// filename was successfully parsed, returns true.  Else return false.
func ParseFileName(filename string) (bool, uint64, FileType) {
	switch filename {
	case "CURRENT":
		return true, 0, CurrentFile
	case "LOCK":
		return true, 0, DBLockFile
	case "LOG":
		fallthrough
	case "LOG.old":
		return true, 0, InfoLogFile
	default:
		break
	}

	if strings.HasPrefix(filename, "MANIFEST-") {
		numstr := filename[9:]
		if num, err := strconv.Atoi(numstr); err == nil {
			return true, uint64(num), DescriptorFile
		}
	} else {
		if len(filename) <= 4 {
			return false, 0, InvalidFile
		}

		// assume name like 000001.log
		if num, err := strconv.Atoi(filename[:len(filename)-4]); err == nil {
			i := strings.LastIndexByte(filename, '.')
			if i < 0 {
				return false, 0, InvalidFile
			}

			suffix := filename[i:]
			switch suffix {
			case ".log":
				return true, uint64(num), LogFile
			case ".ldb":
				return true, uint64(num), TableFile
			case ".dbtmp":
				return true, uint64(num), TempFile
			}
		}
	}

	return false, 0, InvalidFile
}

// Make the CURRENT file point to the descriptor file with the
// specified number.
func SetCurrentFile(dbname string, descriptorNumber uint64) error {
	// Remove leading "dbname/" and add newline to manifest file name
	manifest := DescriptorFileName(dbname, descriptorNumber)
	index := strings.Index(manifest, dbname+"/")
	if index != 0 {
		debug.Panicf("wrong manifest name %s: should start with %s/", manifest, dbname)
	}

	contents := manifest[len(dbname)+1:]
	tmp := TempFileName(dbname, descriptorNumber)

	f, e := os.Create(tmp)
	if e != nil {
		debug.Panicln("create tmp file failed")
	}

	if _, e := f.WriteString(contents + "\n"); e != nil {
		return e
	}

	if e := f.Sync(); e != nil {
		return e
	}

	if e := os.Rename(tmp, CurrentFileName(dbname)); e != nil {
		debug.Panicf("rename failed: %s -> %s", tmp, CurrentFileName(dbname))
	}

	os.Remove(tmp)
	return nil
}

func makeFileName(dbname string, number uint64, suffix string) string {
	return dbname + fmt.Sprintf("/%06v.%s", number, suffix)
}
