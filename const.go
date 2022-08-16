package cooperdb

import "errors"

var (
	ErrEmptyKey = errors.New("cooperdb: the key is empty")

	ErrKeyNotExist = errors.New("cooperdb: key not exist")

	ErrKeyTooLarge = errors.New("cooperdb: key exceeded the max length")

	ErrValueTooLarge = errors.New("cooperdb: value exceeded the max length")

	ErrNilIndexer = errors.New("cooperdb: indexer is nil")

	ErrCfgNotExist = errors.New("cooperdb: the config file not exist")

	ErrReclaimUnreached = errors.New("cooperdb: unused space not reach the threshold")

	ErrExtraContainsSeparator = errors.New("cooperdb: extra contains separator \\0")

	ErrInvalidTtl = errors.New("cooperdb: invalid ttl")

	ErrKeyExpired = errors.New("cooperdb: key is expired")
)
