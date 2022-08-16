package storage

//保存数据库的一些额外信息
type DBMeta struct {
	ActiveWriteOff int64 `json:"active_write_off"` //当前数据文件的写偏移
}
