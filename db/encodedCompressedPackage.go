package db

import (
	"bytes"
	"encoding/gob"
	"os"
	"compress/gzip"
	"io/ioutil"
)

type EncodedCompressedPackage struct {
	name string
	data interface{}
}

func NewEncodedCompressedPackage(name string) *EncodedCompressedPackage {
	return &EncodedCompressedPackage{name, nil}
}

func (p *EncodedCompressedPackage) SetData(data interface{}) {
	p.data = data
}

func (p *EncodedCompressedPackage) Save() error {
	var data bytes.Buffer

	enc := gob.NewEncoder(&data)
	err := enc.Encode(p.data)
	if err != nil {
		return err
	}

	f, err := os.Create(p.name)
	if err != nil {
		return err
	}
	defer f.Close()

	gzipw := gzip.NewWriter(f)
	defer gzipw.Close()
	_, err = gzipw.Write(data.Bytes())

	return err
}

func (p *EncodedCompressedPackage) LoadDecoder() (*gob.Decoder, error) {
	f, err := os.Open(p.name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return gob.NewDecoder(bytes.NewReader(data)), nil
}

