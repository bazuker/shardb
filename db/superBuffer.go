package db

import (
	"bytes"
	"io"
)

type Cutter interface {
	Cut(position, length int) error
}

type SuperBufferInterface interface {
	io.Reader
	io.Seeker
	io.Closer
	Cutter
}

type SuperBuffer struct {
	b      []byte
	buffer *bytes.Buffer
	offset int64
}

func NewSuperBuffer(data []byte) *SuperBuffer {
	if data == nil {
		return nil
	}
	b := bytes.NewBuffer(data)
	return &SuperBuffer{
		b:      b.Bytes(),
		buffer: b,
		offset: 0,
	}
}

func (super *SuperBuffer) Bytes() []byte {
	return super.buffer.Bytes()
}

func (super *SuperBuffer) Cut(position int64, length int) {
	dataRight := super.b[position+int64(length):]
	dataLeft := super.b[:position]
	super.b = append(dataLeft, dataRight...)
	super.buffer = bytes.NewBuffer(super.b)
	super.offset = 0
}

func (super *SuperBuffer) Read(p []byte) (n int, err error) {
	n, err = super.buffer.Read(p)
	super.offset += int64(n)

	return n, err
}

func (super *SuperBuffer) Seek(offset int64, whence int) (ret int64, err error) {
	var newOffset int64
	switch whence {
	case 0:
		newOffset = offset
	case 1:
		newOffset = super.offset + offset
	case 2:
		newOffset = int64(len(super.b)) - offset
	}
	if newOffset == super.offset {
		return newOffset, nil
	}

	super.buffer = bytes.NewBuffer(super.b[newOffset:])
	super.offset = newOffset

	return super.offset, nil
}

func (super *SuperBuffer) Close() error {
	return nil
}
