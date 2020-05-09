// Copyright (c) 2020 Bert Young. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"io"
	"log"
	"sync"
)

type LogBert struct {
	sync.Mutex

	logger *log.Logger
}

var debug *LogBert
var createMutex sync.Mutex

func initLogger(out io.Writer) *LogBert {
	if debug == nil {
		createMutex.Lock()
		if debug == nil {
			log := log.New(out, "", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
			debug = &LogBert{logger: log}
		}
		createMutex.Unlock()
	}

	return debug
}

func (l *LogBert) Printf(format string, v ...interface{}) {
	l.Lock()
	l.logger.Printf(format, v...)
	l.Unlock()
}

func (l *LogBert) Print(v ...interface{}) {
	l.Lock()
	l.logger.Print(v...)
	l.Unlock()
}

func (l *LogBert) Println(v ...interface{}) {
	l.Lock()
	l.logger.Println(v...)
	l.Unlock()
}

func (l *LogBert) Fatalf(format string, v ...interface{}) {
	l.Lock()
	l.logger.Fatalf(format, v...)
	l.Unlock()
}

func (l *LogBert) Fatal(v ...interface{}) {
	l.Lock()
	l.logger.Fatal(v...)
	l.Unlock()
}

func (l *LogBert) Fatalln(v ...interface{}) {
	l.Lock()
	l.logger.Fatalln(v...)
	l.Unlock()
}

func (l *LogBert) Panicf(format string, v ...interface{}) {
	l.Lock()
	l.logger.Panicf(format, v...)
	l.Unlock()
}

func (l *LogBert) Panic(v ...interface{}) {
	l.Lock()
	l.logger.Panic(v...)
	l.Unlock()
}

func (l *LogBert) Panicln(v ...interface{}) {
	l.Lock()
	l.logger.Panicln(v...)
	l.Unlock()
}
