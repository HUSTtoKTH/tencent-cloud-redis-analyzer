package splitter

import (
	"strings"
	"unicode"
)

// Splitter abstraction to split string in fragments
type Splitter interface {
	// Split splits string key to fragments with given strategy
	Split(in string) []string
	Divider() string
}

// SimpleSplitter TODO
// PunctuationSplitter splitting keys by a specific set of symbols (i.e. punctuation)
type SimpleSplitter struct {
	divider string
}

// NewSimpleSplitter  creates PunctuationSplitter
func NewSimpleSplitter(punctuation string) *SimpleSplitter {
	return &SimpleSplitter{divider: punctuation}
}

// Split splits string key to fragments with given strategy
func (s *SimpleSplitter) Split(in string) []string {
	result := strings.Split(in, s.divider)
	for i, v := range result {
		// 包含数字, 非 pattern 字段, 替换掉
		if hasCustomerValue(v) {
			result[i] = "*"
		}
	}

	return clean(result)
}

// Divider TODO
func (s *SimpleSplitter) Divider() string {
	return s.divider
}

func hasCustomerValue(s string) bool {
	b := false
	for _, c := range s {
		if (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || unicode.Is(unicode.Han, c) {
			b = true
			break
		}
	}
	return b
}

// clean 用于除去元素, 相邻* *去掉
func clean(s []string) []string {
	var x []int //x切片用于记录需要删除字符串的下标
	for i := 0; i < len(s)-1; i++ {
		if s[i] == s[i+1] {
			x = append(x, i+1)
		}
	}
	for t, v := range x {
		copy(s[v-t:], s[v+1-t:]) //每覆盖一个前面字符串，下标集体减一，覆盖t次下标减少t
	}
	return s[:len(s)-len(x)]
}
