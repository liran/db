package db

import (
	"fmt"
	"reflect"
	"strings"
)

const tagName = "db"

func (txn *Txn) IndexAdd(model any, field string, val, id any) error {
	baseKey := GenerateIndexBaseKey(model, field, val)

	key := strings.ToLower(fmt.Sprintf("_i:%s:%v", baseKey, id))
	if txn.Has(key) {
		return nil
	}

	if err := txn.Set(key, ""); err != nil {
		return err
	}

	// inc count
	_, err := txn.Inc(fmt.Sprintf("_ic:%s", baseKey), 1)
	return err
}

func (txn *Txn) IndexDel(model any, field string, val, id any) error {
	baseKey := GenerateIndexBaseKey(model, field, val)

	key := fmt.Sprintf("_i:%s:%v", baseKey, id)
	if !txn.Has(key) {
		return nil
	}

	if err := txn.Del(key); err != nil {
		return err
	}

	// dec count
	_, err := txn.Dec(fmt.Sprintf("_ic:%s", baseKey), 1)
	return err
}

func (txn *Txn) IndexList(model any, field string, val any, opts ...*ListOption) (list []string, err error) {
	baseKey := GenerateIndexBaseKey(model, field, val)
	prefix := fmt.Sprintf("_i:%s:", baseKey)

	var opt *ListOption
	if len(opts) > 0 {
		opt = opts[0]
	} else {
		opt = &ListOption{}
	}
	opt.KeyOnly = true

	err = txn.List(prefix,
		func(key string, value []byte) (bool, error) {
			list = append(list, strings.TrimPrefix(key, prefix))
			return false, nil
		},
		opt,
	)
	return
}

func (txn *Txn) IndexCount(model any, field string, val any) (total int64) {
	baseKey := GenerateIndexBaseKey(model, field, val)
	txn.Unmarshal(fmt.Sprintf("_ic:%s", baseKey), &total)
	return
}

func (txn *Txn) IndexClear(model any, field string, val any) error {
	baseKey := GenerateIndexBaseKey(model, field, val)

	// delete list
	prefix := fmt.Sprintf("_i:%s:", baseKey)
	opt := &ListOption{}
	opt.KeyOnly = true
	err := txn.List(prefix,
		func(key string, value []byte) (bool, error) {
			return false, txn.Del(key)
		},
		opt,
	)
	if err != nil {
		return err
	}

	// delete count
	return txn.Del(fmt.Sprintf("_ic:%s", baseKey))
}

// When isCreate is true, it means to create an index, otherwise it means to delete the index
func (txn *Txn) IndexModel(id, model any, isCreate bool) error {
	modelValue := reflect.ValueOf(model)
	k := modelValue.Kind()
	for k == reflect.Pointer || k == reflect.UnsafePointer {
		if modelValue.IsNil() {
			return nil
		}
		modelValue = modelValue.Elem()
		k = modelValue.Kind()
	}
	if k != reflect.Struct {
		return nil
	}

	modelType := modelValue.Type()

	modelName := modelType.Name()

	var action func(model any, field string, val, id any) error
	if isCreate {
		action = txn.IndexAdd
	} else {
		action = txn.IndexDel
	}

	// Iterate over all available fields and read the tag value
	for i := 0; i < modelType.NumField(); i++ {
		fieldType := modelType.Field(i)

		// Get the field tag value
		tag := fieldType.Tag.Get(tagName)
		if tag == "" || !strings.Contains(tag, "index") {
			continue
		}

		// defautl index name is feild name
		indexName := fieldType.Name

		// if specified manually, use the specified name
		multTypes := strings.Split(strings.Trim(tag, ", ;"), ",")
		for _, v := range multTypes {
			if strings.HasPrefix(v, "index") {
				indexs := strings.Split(v, "=")
				if len(indexs) == 2 {
					indexName = strings.TrimSpace(indexs[1])
				}
			}
		}

		fieldValue := modelValue.Field(i)

		kind := fieldValue.Kind()
		switch kind {
		case reflect.Slice:
			if fieldValue.IsNil() {
				continue
			}
			for k := 0; k < fieldValue.Len(); k++ {
				val, ok := ParseReflectValue(fieldValue.Index(k))
				if !ok {
					continue
				}
				// log.Printf("model: %s, index: %s, value: %v, id: %v", modelName, indexName, val, id)
				if err := action(modelName, indexName, val, id); err != nil {
					return err
				}
			}

		case reflect.Map:
			if fieldValue.IsNil() {
				continue
			}
			iter := fieldValue.MapRange()
			for iter.Next() {
				val, ok := ParseReflectValue(iter.Value())
				if !ok {
					continue
				}
				// log.Printf("model: %s, index: %s, value: %v, id: %v", modelName, indexName, val, id)
				if err := action(modelName, indexName, val, id); err != nil {
					return err
				}
			}

		default:
			val, ok := ParseReflectValue(fieldValue)
			if !ok {
				continue
			}
			// log.Printf("model: %s, index: %s, value: %v, id: %v", modelName, indexName, val, id)
			if err := action(modelName, indexName, val, id); err != nil {
				return err
			}
		}
	}

	return nil
}

func (txn *Txn) IndexFirst(model any, field string, val any) (string, error) {
	list, err := txn.IndexList(model, field, val, &ListOption{Limit: 1})
	if err != nil {
		return "", err
	}
	if len(list) > 0 {
		return list[0], nil
	}
	return "", nil
}
