package db

import "errors"

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrStopIterate = errors.New("stop iterate")
)
