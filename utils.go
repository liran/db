package db

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	json "github.com/goccy/go-json"
	"github.com/iancoleman/strcase"
)

func ToBytes(data any) []byte {
	var value []byte
	switch v := data.(type) {
	case []byte:
		value = v
	case string: // Prevent repeated double quotes in the string
		value = []byte(v)
	default:
		// no encode html tag
		buffer := &bytes.Buffer{}
		encoder := json.NewEncoder(buffer)
		encoder.SetEscapeHTML(false)
		encoder.Encode(data)
		buffer.Truncate(buffer.Len() - 1) // remove suffix "\n"
		value = buffer.Bytes()
	}
	return value
}

func GzipCompress(src []byte) ([]byte, error) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	_, err := w.Write(src)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func GzipUncompress(src []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(zr)
}

func PaddingZero(val any, length int) string {
	text := fmt.Sprintf("%v", val)
	diff := length - len(text)
	if diff <= 0 {
		return text
	}

	var b bytes.Buffer
	for i := 0; i < diff; i++ {
		b.WriteString("0")
	}
	b.WriteString(text)
	return b.String()
}

// Whether value can be taken directly
func ParseReflectValue(val reflect.Value) (any, bool) {
	k := val.Kind()

	switch k {
	case reflect.Invalid,
		reflect.Array,
		reflect.Chan,
		reflect.Func,
		reflect.Map,
		reflect.Slice,
		reflect.Struct:
		return nil, false
	}

	if k == reflect.Pointer || k == reflect.UnsafePointer {
		if val.IsNil() {
			return nil, false
		}
		val = val.Elem()
	}

	if k == reflect.Interface {
		if val.IsNil() {
			return nil, false
		}

		switch v := val.Interface().(type) {
		case time.Time:
			return v.Format("2006-01-02"), true
		case *time.Time:
			return v.Format("2006-01-02"), true
		case string:
			if v == "" {
				return "", false
			}
			return strings.ToLower(v), true
		}
	}

	// converte time.Time to YYYY-MM-DD
	if val.Type().String() == "time.Time" {
		return val.Interface().(time.Time).Format("2006-01-02"), true
	}

	// string to lower
	if k == reflect.String {
		v := fmt.Sprintf("%s", val.Interface())
		if v == "" {
			return "", false
		}
		return strings.ToLower(v), true
	}

	return val.Interface(), true
}

func ToModelName(model any) string {
	v := reflect.ValueOf(model)
	k := v.Kind()
	if k == reflect.Invalid {
		return ""
	}
	if k == reflect.Pointer || k == reflect.UnsafePointer {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}

	var name string
	// general data type，such as: int float bool  string .....
	if k >= 1 && k <= 16 || k == 24 {
		name = fmt.Sprintf("%v", model)
	} else {
		name = v.Type().Name()
	}
	return ToSnake(name)
}

func GetBucket(key string) string {
	b, _, _ := strings.Cut(key, ":")
	if b == "" {
		return "default"
	}
	return b
}

func ToSnake(text string) string {
	return strcase.ToSnakeWithIgnore(text, ".")
}

func GenerateIndexBaseKey(model any, field string, val any) string {
	modelName := ToModelName(model)
	snakeField := ToSnake(field)
	return strings.ToLower(fmt.Sprintf("%s:%s:%v", modelName, snakeField, val))
}

func NewModel(model any) any {
	modelVal := reflect.ValueOf(model)
	k := modelVal.Kind()
	for k == reflect.Pointer || k == reflect.UnsafePointer {
		if modelVal.IsNil() {
			return nil
		}
		modelVal = modelVal.Elem()
		k = modelVal.Kind()
	}
	if k != reflect.Struct {
		return nil
	}
	return reflect.New(modelVal.Type()).Interface()
}

func ToEntity[T any](val any) T {
	return val.(T)
}

func ToEntities[T any](items []any) []T {
	var ts []T
	for _, v := range items {
		ts = append(ts, v.(T))
	}
	return ts
}
