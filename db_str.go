package cooperdb

import (
	"db/db/cooperdb/index"
	"db/db/cooperdb/storage"
)

func (db *CooperDB) Set(key, value []byte) error {
	if err := db.doSet(key, value); err != nil {
		return err
	}
	//todo 清除过期时间

	return nil
}

func (db *CooperDB) Get(key []byte) ([]byte, error) {
	keySize := uint32(len(key))
	if keySize == 0 {
		return nil, ErrEmptyKey
	}
	//获取索引
	value := db.Stridx.Get(string(key))
	idx := value.(*index.Indexer)
	if idx == nil {
		return nil, ErrNilIndexer
	}
	db.Mu.RLock()
	defer db.Mu.RUnlock()

	//todo 判断是否过期

	//如果键和值都在内存中则取内存中的值
	if db.Config.IdxMode == KeyValueRamMode {
		return idx.Meta.Value, nil
	}

	if db.Config.IdxMode == KeyOnlyRamMode {
		df := db.ActiveFile
		if db.ActiveFileId != idx.FileId {
			df = db.ArchFiles[idx.FileId]
		}
		if e, err := df.Read(idx.Offset); err != nil {
			return nil, err
		} else {
			return e.Meta.Value, nil
		}
	}
	return nil, ErrKeyNotExist

}

func (db *CooperDB) doSet(key, value []byte) error {
	//检查Key、Value 是否符合规定
	if err := db.checkKeyValue(key, value); err != nil {
		return err
	}
	db.Mu.Lock()
	defer db.Mu.Unlock()

	//创建一个新的entry
	e := storage.NewEntryNoExtra(key, value, String, StringSet)
	if err := db.store(e); err != nil {
		return err
	}
	//todo 增加索引
	//数据索引
	idx := &index.Indexer{
		Meta: &storage.Meta{
			KeySize: uint32(len(e.Meta.Key)),
			Key:     e.Meta.Key,
		},
		FileId:    db.ActiveFileId,
		EntrySize: e.Size(),
		Offset:    db.ActiveFile.Offset - int64(e.Size()),
	}
	if err := db.buildIndex(e, idx); err != nil {
		return err
	}

	return nil
}

func (db *CooperDB) checkKeyValue(key []byte, value ...[]byte) error {
	keySize := uint32(len(key))
	if keySize == 0 {
		return ErrEmptyKey
	}

	config := db.Config
	if keySize > config.MaxKeySize {
		return ErrKeyTooLarge
	}

	for _, v := range value {
		if uint32(len(v)) > config.MaxValueSize {
			return ErrValueTooLarge
		}
	}

	return nil
}

func Get(key []byte) []byte {

	return []byte{}
}
