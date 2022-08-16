package cooperdb

import (
	"db/db/cooperdb/index"
	"db/db/cooperdb/storage"
	"sync"
)

type (
	CooperDB struct {
		ActiveFile   *storage.DBFile //当前活跃文件
		ActiveFileId uint32          //活跃文件id
		ArchFiles    ArchivedFiles   //已封存文件
		Stridx       *index.Stridx   //字符串索引列表
		Config       Config          //数据库配置
		Mu           sync.RWMutex    //mutex
		Meta         *storage.DBMeta //数据库配置额外信息
	}

	//已封存的文件定义
	ArchivedFiles map[uint32]*storage.DBFile
)

//写数据
func (db *CooperDB) store(e *storage.Entry) error {
	config := db.Config
	//如果数据文件空间不够，则关闭该文件，并新打开一个文件
	if db.ActiveFile.Offset+int64(e.Size()) > config.BlockSize {
		if err := db.ActiveFile.Close(true); err != nil {
			return err
		}
		//保存旧的文件
		db.ArchFiles[db.ActiveFileId] = db.ActiveFile
		activeFileId := db.ActiveFileId + 1

		//新建一个文件
		if dbFile, err := storage.NewDBFile(config.DirPath, activeFileId, config.RwMethod, config.BlockSize); err != nil {
			return err
		} else {
			db.ActiveFile = dbFile
			db.ActiveFileId = activeFileId
			db.Meta.ActiveWriteOff = 0
		}

	}
	//写入数据至文件中
	if err := db.ActiveFile.Write(e); err != nil {
		return err
	}
	db.Meta.ActiveWriteOff = db.ActiveFile.Offset

	//数据持久化
	if config.Sync {
		if err := db.ActiveFile.Sync(); err != nil {
			return err
		}
	}
	return nil
}

//建立索引
func (db *CooperDB) buildIndex(e *storage.Entry, idx *index.Indexer) error {

	if db.Config.IdxMode == KeyValueRamMode {
		idx.Meta.Value = e.Meta.Value
		idx.Meta.ValueSize = uint32(len(e.Meta.Value))
	}

	switch e.Type {
	case storage.String:
		db.buildStringIndex(idx, e.Mark)
	}

	return nil
}
