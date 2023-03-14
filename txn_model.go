package db

import (
	"encoding/json"
	"errors"
	"fmt"
)

func (txn *Txn) ModelNextID(model any, length int) string {
	modelName := ToModelName(model)
	if modelName == "" {
		return ""
	}

	c, _ := txn.Inc(fmt.Sprintf("_counter:%s", modelName), 1)

	txn.Set(fmt.Sprintf("_id_len:%s", modelName), length)

	return PaddingZero(c, length)
}

func (txn *Txn) ModelCounter(model any) (count int64) {
	modelName := ToModelName(model)
	if modelName == "" {
		return 0
	}

	txn.Unmarshal(fmt.Sprintf("_counter:%s", modelName), &count)
	return
}

func (txn *Txn) ModelTotal(model any) (count int64) {
	modelName := ToModelName(model)
	if modelName == "" {
		return 0
	}

	txn.Unmarshal(fmt.Sprintf("_total:%s", modelName), &count)
	return
}

func (txn *Txn) ModelIdLength(model any) (length int) {
	modelName := ToModelName(model)
	if modelName == "" {
		return 0
	}

	txn.Unmarshal(fmt.Sprintf("_id_len:%s", modelName), &length)
	return
}

func (txn *Txn) ModelSet(model, id any) error {
	modelName := ToModelName(model)
	if modelName == "" {
		return nil
	}

	// update index
	old := NewModel(model)
	if old == nil {
		return nil
	}

	key := fmt.Sprintf("%s:%v", modelName, id)
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

func (txn *Txn) ModelDel(model, id any) error {
	modelName := ToModelName(model)
	if modelName == "" {
		return nil
	}

	m := NewModel(model)
	if m == nil {
		return nil
	}

	key := fmt.Sprintf("%s:%v", modelName, id)

	// delete index
	err := txn.Unmarshal(key, m)
	if err != nil {
		if !errors.Is(err, ErrKeyNotFound) {
			return err
		}
		return nil
	}
	if err = txn.IndexModel(id, m, false); err != nil {
		return err
	}

	// dec total
	if _, err := txn.Dec(fmt.Sprintf("_total:%s", modelName), 1); err != nil {
		return err
	}

	// del model
	return txn.Del(key)
}

func (txn *Txn) ModelUpdate(model, id any, cb func(mPointer any) error) error {
	m := NewModel(model)
	if m == nil {
		return ErrKeyNotFound
	}

	modelName := ToModelName(m)
	if modelName == "" {
		return ErrKeyNotFound
	}

	key := fmt.Sprintf("%s:%v", modelName, id)
	err := txn.Unmarshal(key, m)
	if err != nil {
		return err
	}

	if err := cb(m); err != nil {
		return err
	}

	return txn.ModelSet(m, id)
}

func (txn *Txn) ModelGet(model, id any) (any, error) {
	m := NewModel(model)
	if m == nil {
		return nil, ErrKeyNotFound
	}

	modelName := ToModelName(m)
	if modelName == "" {
		return nil, ErrKeyNotFound
	}

	key := fmt.Sprintf("%s:%v", modelName, id)
	err := txn.Unmarshal(key, m)
	return m, err
}

func (txn *Txn) ModelUnmarshal(model, id any) error {
	modelName := ToModelName(model)
	if modelName == "" {
		return ErrKeyNotFound
	}

	key := fmt.Sprintf("%s:%v", modelName, id)
	return txn.Unmarshal(key, model)
}

func (txn *Txn) ModelList(model any, limit int, begin string, reverse bool) (list []any, err error) {
	modelName := ToModelName(model)
	if modelName == "" {
		return nil, nil
	}

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
		m := NewModel(model)
		if err := json.Unmarshal(value, m); err != nil {
			return true, err
		}
		list = append(list, m)
		return false, nil
	}, opt)
	return
}

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
