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
	compressionLevel int
}

func NewCompressedPackage(name string) *CompressedPackage {
	return &CompressedPackage{name, nil, gzip.BestCompression}
}

func (p *CompressedPackage) SetData(data []byte) {
	p.data = data
}

func (p *CompressedPackage) SetCompressionLevel(level int) {
	p.compressionLevel = level
}

func (p *CompressedPackage) Save() error {
	data := bytes.NewBuffer(p.data)
	f, err := os.Create(p.name)
	if err != nil {
		return err
	}
	defer f.Close()

	gzipw, _ := gzip.NewWriterLevel(f, p.compressionLevel)
	defer gzipw.Close()
	_, err = gzipw.Write(data.Bytes())

	return err
}

func (p *CompressedPackage) Load() ([]byte, error) {
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
