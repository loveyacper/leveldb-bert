package leveldb

import (
	"bytes"
	"fmt"
)

type WriteBatch struct {
	rep []byte
}

const (
	// 8byte seq + 4byte count
	kWriteBatchHeader = 12
)

func NewWriteBatch() *WriteBatch {
	wb := &WriteBatch{}
	wb.Clear()
	return wb
}

func (wb *WriteBatch) Clear() {
	wb.rep = make([]byte, kWriteBatchHeader)
}

func (wb *WriteBatch) Put(key, value []byte) {
	buf := bytes.NewBuffer(wb.rep)
	buf.WriteByte(byte(TypeValue))
	PutLengthPrefixedSlice(buf, key)
	PutLengthPrefixedSlice(buf, value)

	wb.rep = buf.Bytes()
	wb.SetCount(wb.Count() + 1)
}

func (wb *WriteBatch) Delete(key []byte) {
	buf := bytes.NewBuffer(wb.rep)
	buf.WriteByte(byte(TypeDeletion))
	PutLengthPrefixedSlice(buf, key)

	wb.rep = buf.Bytes()
	wb.SetCount(wb.Count() + 1)
}

func (wb *WriteBatch) Count() int {
	buf := bytes.NewBuffer(wb.rep[8:kWriteBatchHeader])
	if c, err := DecodeFixed32(buf); err != nil {
		panic(fmt.Sprintf("WriteBatch.Count error %v", err))
	} else {
		return int(c)
	}

	return -1
}

func (wb *WriteBatch) SetCount(c int) {
	buf := bytes.NewBuffer(wb.rep[8:8])
	PutFixed32(buf, uint32(c))
}

func (wb *WriteBatch) Sequence() SequenceNumber {
	buf := bytes.NewBuffer(wb.rep[0:8])
	if s, err := DecodeFixed64(buf); err != nil {
		panic(fmt.Sprintf("WriteBatch.Sequence error %v", err))
	} else {
		return SequenceNumber(s)
	}

	return kMaxSequenceNumber
}

func (wb *WriteBatch) SetSequence(s SequenceNumber) {
	buf := bytes.NewBuffer(wb.rep[0:0])
	PutFixed32(buf, uint32(s))
}

func (wb *WriteBatch) Contents() []byte {
	return wb.rep
}

func (wb *WriteBatch) SetContents(contents []byte) {
	wb.rep = contents
}

func (wb *WriteBatch) ByteSize() int {
	return len(wb.rep)
}

func (wb *WriteBatch) Append(src *WriteBatch) {
	wb.SetCount(wb.Count() + src.Count())
	wb.rep = append(wb.rep, src.rep[kWriteBatchHeader:]...)
}

func (wb *WriteBatch) ForEach(wbp WriteBatchProcessor) Status {
	counts := wb.Count()
	seq := wb.Sequence()

	buf := bytes.NewBuffer(wb.rep[kWriteBatchHeader:])
	realCount := 0
	for buf.Len() != 0 {
		tp, err := buf.ReadByte()
		if err != nil {
			return NewStatus(Corruption, err.Error())
		}

		if tp != byte(TypeDeletion) && tp != byte(TypeValue) {
			return NewStatus(Corruption, fmt.Sprintf("Error type %v when ForEach", tp))
		}

		var key []byte
		err = GetLengthPrefixedSlice(buf, &key)
		if err != nil {
			return NewStatus(Corruption, err.Error())
		}

		var value []byte
		if tp == TypeValue {
			err = GetLengthPrefixedSlice(buf, &value)
			if err != nil {
				return NewStatus(Corruption, err.Error())
			}
		}

		wbp.ProcessWriteBatch(seq, ValueType(tp), key, value)
		seq++
		realCount++
	}

	if realCount != counts {
		return NewStatus(Corruption, fmt.Sprintf("WriteBatch Foreach: realCount %v but count is %v", realCount, counts))
	}

	return NewStatus(OK)
}

// For iterate writebatch and process each write request
type WriteBatchProcessor interface {
	ProcessWriteBatch(seq SequenceNumber, tp ValueType, key, value []byte)
}
