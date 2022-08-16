package index

type Stridx struct {
	m map[string]interface{}
}

func (t *Stridx) Put(key string, value interface{}) {
	t.m[key] = value
}

func (t *Stridx) Get(key string) interface{} {
	if value, ok := t.m[key]; ok {
		return value
	}

	return nil
}
