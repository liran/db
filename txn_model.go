package db

import (
	"encoding/json"
	"errors"
	"fmt"

	clone "github.com/huandu/go-clone"
)

func (txn *Txn) ModelNextID(model any, length int) string {
	modelName := ToModelName(model)
	c, _ := txn.Inc(fmt.Sprintf("_counter:%s", modelName), 1)

	txn.Set(fmt.Sprintf("_id_len:%s", modelName), length)

	return PaddingZero(c, length)
}

func (txn *Txn) ModelCounter(model any) (count int64) {
	modelName := ToModelName(model)
	txn.Unmarshal(fmt.Sprintf("_counter:%s", modelName), &count)
	return
}

func (txn *Txn) ModelTotal(model any) (count int64) {
	modelName := ToModelName(model)
	txn.Unmarshal(fmt.Sprintf("_total:%s", modelName), &count)
	return
}

func (txn *Txn) ModelIdLength(model any) (length int) {
	modelName := ToModelName(model)
	txn.Unmarshal(fmt.Sprintf("_id_len:%s", modelName), &length)
	return
}

// model is a pointer of T
func (txn *Txn) ModelSet(model, id any) error {
	modelName := ToModelName(model)
	key := fmt.Sprintf("%s:%v", modelName, id)

	// update index
	old := clone.Clone(model)
	err := txn.Unmarshal(key, old)
	if err != nil {
		if !errors.Is(err, ErrKeyNotFound) {
			return err
		}

		// inc total
		if _, err := txn.Inc(fmt.Sprintf("_total:%s", modelName), 1); err != nil {
			return err
		}
	} else {
		if err = txn.IndexModel(id, old, false); err != nil {
			return err
		}
	}
	if err := txn.IndexModel(id, model, true); err != nil {
		return err
	}

	// save model
	return txn.Set(key, model)
}

// model is a pointer of T
func (txn *Txn) ModelDel(model, id any) error {
	modelName := ToModelName(model)
	key := fmt.Sprintf("%s:%v", modelName, id)

	// update index
	err := txn.Unmarshal(key, model)
	if err != nil {
		if !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		return nil
	}
	if err = txn.IndexModel(id, model, false); err != nil {
		return err
	}

	// dec total
	if _, err := txn.Dec(fmt.Sprintf("_total:%s", modelName), 1); err != nil {
		return err
	}

	// del model
	return txn.Del(key)
}

// model is a pointer of T
func (txn *Txn) ModelUpdate(model, id any, cb func(m any) error) error {
	modelName := ToModelName(model)
	key := fmt.Sprintf("%s:%v", modelName, id)

	m := clone.Clone(model)
	err := txn.Unmarshal(key, m)
	if err != nil {
		return err
	}

	if err := cb(m); err != nil {
		return err
	}

	return txn.ModelSet(m, id)
}

// model is a pointer of T
func (txn *Txn) ModelGet(model, id any) (any, error) {
	modelName := ToModelName(model)
	key := fmt.Sprintf("%s:%v", modelName, id)

	m := clone.Clone(model)
	err := txn.Unmarshal(key, m)
	return m, err
}

// model is a pointer of T
func (txn *Txn) ModelList(model any, limit int, begin string, reverse bool) (list []any, err error) {
	modelName := ToModelName(model)
	prefix := fmt.Sprintf("%s:", modelName)

	containBegin := false
	if begin == "" && reverse {
		last := txn.ModelCounter(model)
		begin = fmt.Sprintf("%s%s", prefix, PaddingZero(last, txn.ModelIdLength(model)))
		containBegin = true
	}
	opt := &ListOption{
		Begin:        begin,
		ContainBegin: containBegin,
		Limit:        limit,
		Reverse:      reverse,
	}
	err = txn.List(prefix, func(key string, value []byte) (bool, error) {
		m := clone.Clone(model)
		if err := json.Unmarshal(value, m); err != nil {
			return true, err
		}
		list = append(list, m)
		return false, nil
	}, opt)
	return
}

// model is a pointer of T
func (txn *Txn) ModelIndexList(model any, feild string, val any, opts ...*ListOption) (list []any, err error) {
	ids, err := txn.IndexList(model, feild, val, opts...)
	if err != nil {
		return nil, err
	}
	for _, v := range ids {
		o, err := txn.ModelGet(model, v)
		if err != nil {
			return nil, err
		}
		list = append(list, o)
	}
	return
}
