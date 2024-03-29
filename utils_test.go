package db

import (
	"bytes"
	"log"
	"reflect"
	"testing"
	"time"
)

func TestToBytes(t *testing.T) {
	log.Println(ToBytes(1))
}

func TestCompress(t *testing.T) {
	src := []byte("hello world!")
	dst, err := GzipCompress(src)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := GzipUncompress(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(src, raw) {
		t.Fatal("GzipUncompress !Equal")
	}
	log.Printf("[%d] [%d] %s", len(src), len(dst), raw)
}

func TestPaddingZero(t *testing.T) {
	log.Println(PaddingZero(1000, 8))
}

func BenchmarkPaddingZero(b *testing.B) {
	for i := 0; i < b.N; i++ {
		PaddingZero(1000, 10)
	}
}

func TestParseReflectValue(t *testing.T) {
	type Status string
	var myStatus Status = "my_status"

	type TestData struct {
		input        reflect.Value
		expectedAny  interface{}
		expectedBool bool
	}

	testCases := []TestData{
		{
			input:        reflect.ValueOf(10),
			expectedAny:  10,
			expectedBool: true,
		},
		{
			input:        reflect.ValueOf(1.1),
			expectedAny:  1.1,
			expectedBool: true,
		},
		{
			input:        reflect.ValueOf(false),
			expectedAny:  false,
			expectedBool: true,
		},
		{
			input:        reflect.ValueOf(nil),
			expectedAny:  nil,
			expectedBool: false,
		},
		{
			input:        reflect.ValueOf("test"),
			expectedAny:  "test",
			expectedBool: true,
		},
		{
			input:        reflect.ValueOf([]int{1, 2, 3}),
			expectedAny:  nil,
			expectedBool: false,
		},
		{
			input:        reflect.ValueOf(make(chan int)),
			expectedAny:  nil,
			expectedBool: false,
		},
		{
			input:        reflect.ValueOf(func() {}),
			expectedAny:  nil,
			expectedBool: false,
		},
		{
			input:        reflect.ValueOf(struct{}{}),
			expectedAny:  nil,
			expectedBool: false,
		},
		{
			input:        reflect.ValueOf(myStatus),
			expectedAny:  string(myStatus),
			expectedBool: true,
		},
	}

	for _, tc := range testCases {
		actualAny, actualBool := ParseReflectValue(tc.input)

		if actualAny != tc.expectedAny {
			t.Errorf("expected '%v' but got '%v'", tc.expectedAny, actualAny)
		}

		if actualBool != tc.expectedBool {
			t.Errorf("expected '%v' but got '%v'", tc.expectedBool, actualBool)
		}
	}
}

func TestToModelName(t *testing.T) {
	log.Println(ToModelName(&time.Time{}))

	type User struct{}
	log.Println(ToModelName(User{}))
	log.Println(ToModelName(&User{}))

	var user *User
	log.Println(ToModelName(user))

	now := time.Now()
	log.Println(ToModelName(now))

	log.Println(ToModelName(nil))

	type MapIII map[string]string
	log.Println(ToModelName(MapIII{}))
	log.Println(ToModelName(map[string]int{}))

	log.Println(ToModelName([0]struct{}{}))
	log.Println(ToModelName(struct{}{}))
	log.Println(ToModelName(struct{ A string }{"a"}))

	log.Println(ToModelName("abc"))
	log.Println(ToModelName(12.2))
	log.Println(ToModelName(10))
	log.Println(ToModelName(false))
}

func TestParseKey(t *testing.T) {
	type TestData struct {
		input    string
		expected string
	}

	testCases := []TestData{
		{
			input:    "key:value",
			expected: "key",
		},
		{
			input:    "foo:bar:baz",
			expected: "foo",
		},
		{
			input:    "_12adf:bar:baz:324",
			expected: "_12adf",
		},
		{
			input:    "no-colon",
			expected: "no-colon",
		},
		{
			input:    "",
			expected: "default",
		},
		{
			input:    ":bar",
			expected: "default",
		},
		{
			input:    "bar::",
			expected: "bar",
		},
		{
			input:    "bar:",
			expected: "bar",
		},
	}

	for _, tc := range testCases {
		actual := GetBucket(tc.input)
		if actual != tc.expected {
			t.Errorf("expected '%v' but got '%v'", tc.expected, actual)
		}
	}
}

func TestToSnake(t *testing.T) {
	type TestData struct {
		input    string
		expected string
	}

	testCases := []TestData{
		{
			input:    "key value",
			expected: "key_value",
		},
		{
			input:    "chatGPTAccount",
			expected: "chat_gpt_account",
		},
		{
			input:    "CdasfAB",
			expected: "cdasf_ab",
		},
		{
			input:    "12.3",
			expected: "12.3",
		},
		{
			input:    "12,3",
			expected: "12,3",
		},
		{
			input:    "12-3",
			expected: "12_3",
		},
	}

	for _, tc := range testCases {
		actual := ToSnake(tc.input)
		if actual != tc.expected {
			t.Errorf("expected '%v' but got '%v'", tc.expected, actual)
		}
	}
}
