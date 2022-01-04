// Package trie TODO
package trie

import (
	"strings"

	"github.com/HUSTtoKTH/redis-analyzer/src/matcher"
	"github.com/HUSTtoKTH/redis-analyzer/src/splitter"
)

// NewTypeTrie created Trie
func NewTypeTrie(splitter splitter.Splitter) *TypeTrie {
	node := NewNode()
	node.AddAggregator(NewAggregator())
	return &TypeTrie{
		root:     node,
		splitter: splitter,
		matcher: matcher.NewMatcher([]string{
			"data_proxyc_*_uin",
			"data_proxyc_*_uid",
			"data_proxyc_user_uid_*",
			"data_proxyc_uid_oids_*",
			"data_proxyc_*_parent",
			"data_proxyc_*_childs",
			"data_proxyc_user_login_*",
		}),
	}
}

// TypeTrie stores data about keys in a prefix tree
type TypeTrie struct {
	root     *Node
	splitter splitter.Splitter
	matcher  *matcher.Matcher
}

// Add adds information about another key with set of params
func (t *TypeTrie) Add(key, keyType string, paramValues ...ParamValue) {
	curNode := t.root
	var nextNode *Node
	if childNode := curNode.GetChild(keyType); childNode == nil {
		nextNode = NewNode()
		nextNode.AddAggregator(NewAggregator())
		curNode.AddChild(keyType, nextNode)
	} else {
		nextNode = childNode
	}

	pattern := t.matcher.Match(key)
	if pattern == "" {
		keyPieces := t.splitter.Split(key)
		pattern = strings.Join(keyPieces, t.splitter.Divider())
	}

	var finalNode *Node
	if childNode := nextNode.GetChild(pattern); childNode == nil {
		finalNode = NewNode()
		finalNode.AddAggregator(NewAggregator())
		nextNode.AddChild(pattern, finalNode)
	} else {
		finalNode = childNode
	}

	for _, p := range paramValues {
		curNode.Aggregator().Add(p.Param, p.Value)
		nextNode.Aggregator().Add(p.Param, p.Value)
		finalNode.Aggregator().Add(p.Param, p.Value)
	}
}

// Root returns root of the trie
func (t *TypeTrie) Root() *Node {
	return t.root
}

// Clean TODO 清除 count ==1 的 pattern
func (t *TypeTrie) Clean(minPatternNumber int) {
	for _, childNode := range t.root.Children {
		otherNode := NewNode()
		otherNode.AddAggregator(NewAggregator())
		childNode.AddChild("other", otherNode)
		for key, child := range childNode.Children {
			paramMap := child.Aggregator().Params
			if paramMap[KeysCount] <= int64(minPatternNumber) {
				for k, v := range paramMap {
					otherNode.Aggregator().Add(k, v)
				}
				delete(childNode.Children, key)
			}
		}
	}
}
