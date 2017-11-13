package db

import (
	"bytes"
	"os"
	"compress/gzip"
	"io/ioutil"
)

type CompressedPackage struct {
	name string
	data []byte
}

func NewCompressedPackage(name string) *CompressedPackage {
	return &CompressedPackage{name, nil}
}

func (p *CompressedPackage) SetData(data []byte) {
	p.data = data
}

func (p *CompressedPackage) Save() error {
	data := bytes.NewBuffer(p.data)
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

func (p *CompressedPackage) LoadDecoder() ([]byte, error) {
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

	return ioutil.ReadAll(reader)
}
