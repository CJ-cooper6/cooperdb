package cooperdb

import (
	"db/db/cooperdb/index"
)

//数据类型定义
type DataType = uint16

const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

//字符串相关操作标识
const (
	StringSet uint16 = iota
)

//建立字符串索引
func (db *CooperDB) buildStringIndex(idx *index.Indexer, opt uint16) {
	if db.Stridx == nil || idx == nil {
		return
	}
	//todo
	// now := uint32(time.Now().Unix())
	// if deadline, exist := db.expires[string(idx.Meta.Key)]; exist && deadline <= now {
	// 	return
	// }
	key := string(idx.Meta.Key)
	switch opt {
	case StringSet:
		db.Stridx.Put(key, idx)
	}
}
