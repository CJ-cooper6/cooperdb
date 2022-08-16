package storage

import (
	"errors"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/roseduan/mmap-go"
)

const (
	//默认的创建文件权限
	FilePerm = 0644

	//默认数据文件名称格式化
	DBFileFormatName = "%09d.data"

	PathSeparator = string(os.PathSeparator)
)

var (
	ErrEmptyEntry = errors.New("storage/db_file: entry or the Key of entry is empty")
)

type FileRWMethod uint8

const (

	//FileIO表示文件数据读写使用系统标准IO
	FileIO FileRWMethod = 1

	//MMap表示文件数据读写使用Mmap
	//MMap指的是将文件或其他设备映射至内存，具体可参考Wikipedia上的解释 https://en.wikipedia.org/wiki/Mmap
	MMap
)

type DBFile struct {
	Id     uint32
	path   string
	File   *os.File
	mmap   mmap.MMap
	Offset int64
	method FileRWMethod
}

//新建一个数据读写文件，如果是MMap，则需要Truncate文件并进行加载
func NewDBFile(path string, fileId uint32, method FileRWMethod, blockSize int64) (*DBFile, error) {
	filePath := path + PathSeparator + fmt.Sprintf(DBFileFormatName, fileId)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	df := &DBFile{Id: fileId, path: path, Offset: 0, method: method}

	if method == FileIO {
		df.File = file
	} else {
		if err = file.Truncate(blockSize); err != nil {
			return nil, err
		}
		if m, err := mmap.Map(file, os.O_RDWR, 0); err != nil {
			return nil, err
		} else {
			df.mmap = m
		}
	}

	return df, nil
}

//关闭文件操作，传入参数sync 代表是否持久化数据
func (df *DBFile) Close(sync bool) error {
	var err error
	if sync {
		err = df.Sync()
	}

	if df.File != nil {
		err = df.File.Close()
	}
	if df.mmap != nil {
		err = df.mmap.Unmap()
	}
	return err
}

//数据持久化
func (df *DBFile) Sync() error {
	var err error
	if df.File != nil {
		err = df.File.Sync()
	}
	if df.mmap != nil {
		err = df.mmap.Flush()
	}
	return err
}

//从文件的offset处开始写数据
func (df *DBFile) Write(e *Entry) error {
	if e == nil || e.Meta.KeySize == 0 {
		return ErrEmptyEntry
	}

	method := df.method
	writeOff := df.Offset
	if encVal, err := e.Encode(); err != nil {
		return err
	} else {
		if method == FileIO {
			if _, err := df.File.WriteAt(encVal, writeOff); err != nil {
				return err
			}
		}

		if method == MMap {
			copy(df.mmap[writeOff:], encVal)
		}
	}

	df.Offset += int64(e.Size())
	return nil
}

//从数据文件中读数据，offset是读的起始位置
func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	var buf []byte
	if buf, err = df.readBuf(offset, int64(entryHeaderSize)); err != nil {
		return
	}

	if e, err = Decode(buf); err != nil {
		return
	}

	offset += entryHeaderSize
	if e.Meta.KeySize > 0 {
		var key []byte
		if key, err = df.readBuf(offset, int64(e.Meta.KeySize)); err != nil {
			return
		}
		e.Meta.Key = key
	}

	offset += int64(e.Meta.KeySize)
	if e.Meta.ValueSize > 0 {
		var val []byte
		if val, err = df.readBuf(offset, int64(e.Meta.ValueSize)); err != nil {
			return
		}
		e.Meta.Value = val
	}

	offset += int64(e.Meta.ValueSize)
	if e.Meta.ExtraSize > 0 {
		var val []byte
		if val, err = df.readBuf(offset, int64(e.Meta.ExtraSize)); err != nil {
			return
		}
		e.Meta.Extra = val
	}

	checkCrc := crc32.ChecksumIEEE(e.Meta.Value)
	if checkCrc != e.crc32 {
		return nil, ErrInvalidCrc
	}

	return
}

func (df *DBFile) readBuf(offset int64, n int64) ([]byte, error) {
	buf := make([]byte, n)

	if df.method == FileIO {
		_, err := df.File.ReadAt(buf, offset)
		if err != nil {
			return nil, err
		}
	}

	if df.method == MMap && offset <= int64(len(df.mmap)) {
		copy(buf, df.mmap[offset:])
	}

	return buf, nil
}
