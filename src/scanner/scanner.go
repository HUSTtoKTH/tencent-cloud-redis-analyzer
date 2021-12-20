// Package scanner TODO
package scanner

import (
	"context"

	"github.com/HUSTtoKTH/redis-analyzer/src/progress"
	"github.com/HUSTtoKTH/redis-analyzer/src/trie"
	"github.com/rs/zerolog"
)

// RedisServiceInterface abstraction to access redis
type RedisServiceInterface interface {
	ScanKeys(ctx context.Context, options ScanOptions) <-chan *KeyInfo
	GetKeysCount(ctx context.Context) (int64, error)
	GetMemoryUsage(ctx context.Context, key KeyInfo) (int64, error)
	GetKeyType(ctx context.Context, key *KeyInfo)
	GetTypeBatch(ctx context.Context, keys []*KeyInfo)
	GetMemoryUsageBatch(ctx context.Context, keys []*KeyInfo)
}

// ScanOptions TODO
type ScanOptions struct {
	Pattern   string
	ScanCount int
	Throttle  int
}

// KeyInfo TODO
type KeyInfo struct {
	Key       string
	Node      string
	Type      string
	BytesSize int64
}

// RedisScanner scans redis keys and puts them in a trie
type RedisScanner struct {
	redisService RedisServiceInterface
	scanProgress progress.ProgressWriter
	logger       zerolog.Logger
}

// NewScanner creates RedisScanner
func NewScanner(redisService RedisServiceInterface, scanProgress progress.ProgressWriter, logger zerolog.Logger) *RedisScanner {
	return &RedisScanner{
		redisService: redisService,
		scanProgress: scanProgress,
		logger:       logger,
	}
}

// Scan initiates scanning process
func (s *RedisScanner) Scan(options ScanOptions, result *trie.TypeTrie) {
	totalCount := s.getKeysCount()
	var count int
	s.scanProgress.Start(totalCount)
	keys := []*KeyInfo{}
	for key := range s.redisService.ScanKeys(context.Background(), options) {
		if count >= 1000000 {
			break
		}
		s.scanProgress.Increment()
		count++
		keys = append(keys, key)
		if len(keys) == 100 {
			s.redisService.GetMemoryUsageBatch(context.Background(), keys)
			s.redisService.GetTypeBatch(context.Background(), keys)
			for _, key := range keys {
				result.Add(
					key.Key,
					key.Type,
					trie.ParamValue{Param: trie.BytesSize, Value: key.BytesSize},
					trie.ParamValue{Param: trie.KeysCount, Value: 1},
				)
			}
			keys = []*KeyInfo{}
		}
	}
	s.redisService.GetMemoryUsageBatch(context.Background(), keys)
	s.redisService.GetTypeBatch(context.Background(), keys)
	// time.Sleep(100 * time.Millisecond)
	for _, key := range keys {
		result.Add(
			key.Key,
			key.Type,
			trie.ParamValue{Param: trie.BytesSize, Value: key.BytesSize},
			trie.ParamValue{Param: trie.KeysCount, Value: 1},
		)
	}
	s.scanProgress.Stop()
}

func (s *RedisScanner) getKeysCount() int64 {
	res, err := s.redisService.GetKeysCount(context.Background())
	if err != nil {
		s.logger.Error().Err(err).Msgf("Error getting number of keys")
		return 0
	}
	s.logger.Info().Msgf("key number: %d", res)
	return res
}
