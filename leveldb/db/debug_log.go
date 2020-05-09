// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"io"
	"log"
	"sync"
)

var debug *log.Logger
var createMutex sync.Mutex

func initLogger(out io.Writer) *log.Logger {
	if debug == nil {
		createMutex.Lock()
		if debug == nil {
			debug = log.New(out, "", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
		}
		createMutex.Unlock()
	}

	return debug
}
