// Package matcher TODO
package matcher

import (
	"regexp"
	"strings"
)

// Matcher TODO
type Matcher struct {
	pattern []string
	regexps []*regexp.Regexp
}

// NewMatcher TODO
func NewMatcher(pattern []string) *Matcher {
	regexps := []*regexp.Regexp{}
	for _, p := range pattern {
		p = prepareString(p)
		r, _ := regexp.Compile(p)
		regexps = append(regexps, r)
	}
	return &Matcher{
		pattern: pattern,
		regexps: regexps,
	}
}

// parse redis pattern to go pattern
func prepareString(p string) string {
	return strings.Replace(p, `*`, `.*`, -1)
}

// Match TODO
func (m *Matcher) Match(s string) string {
	for i, r := range m.regexps {
		if r.MatchString(s) {
			return m.pattern[i]
		}
	}
	return ""
}
