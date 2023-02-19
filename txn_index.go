package db

import (
	"fmt"
	"strings"
)

func (t *Txn) IndexAdd(model, field string, val any, id string) error {
	if err := t.Set(fmt.Sprintf("_i:%s:%s:%v:%s", model, field, val, id), ""); err != nil {
		return err
	}

	// inc count
	_, err := t.Inc(fmt.Sprintf("_ic:%s:%s:%v", model, field, val), 1)
	return err
}

func (t *Txn) IndexDel(model, field string, val any, id string) error {
	key := fmt.Sprintf("_i:%s:%s:%v:%s", model, field, val, id)
	if !t.Has(key) {
		return nil
	}

	if err := t.Del(key); err != nil {
		return err
	}

	// dec count
	_, err := t.Dec(fmt.Sprintf("_ic:%s:%s:%v", model, field, val), 1)
	return err
}

func (t *Txn) IndexList(model, field string, val any, opts ...*ListOption) (list []string, err error) {
	prefix := fmt.Sprintf("_i:%s:%s:%v:", model, field, val)

	var opt *ListOption
	if len(opts) > 0 {
		opt = opts[0]
	} else {
		opt = &ListOption{}
	}
	opt.KeyOnly = true

	err = t.List(prefix,
		func(key string, value []byte) (bool, error) {
			list = append(list, strings.TrimPrefix(key, prefix))
			return false, nil
		},
		opt,
	)
	return
}

func (t *Txn) IndexCount(model, field string, val any) (total int64) {
	t.Unmarshal(fmt.Sprintf("_ic:%s:%s:%v", model, field, val), &total)
	return
}

func (t *Txn) IndexClear(model, field string, val any) error {
	prefix := fmt.Sprintf("_i:%s:%s:%v:", model, field, val)

	opt := &ListOption{}
	opt.KeyOnly = true

	err := t.List(prefix,
		func(key string, value []byte) (bool, error) {
			return false, t.Del(key)
		},
		opt,
	)
	if err != nil {
		return err
	}

	return t.Del(fmt.Sprintf("_ic:%s:%s:%v", model, field, val))
}
