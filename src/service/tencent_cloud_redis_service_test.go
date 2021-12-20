package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/HUSTtoKTH/redis-analyzer/src/scanner"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

/**
腾讯云 redis 集群版, 无 mock 包. 直连 redis 测试
**/
func TestTencentCloudRedisService_ScanKeys(t *testing.T) {
	redisAddr := "11.168.176.16:6379"
	c := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "Tencent88", // no password set
	})
	s := NewTencentCloudRedisService(c)
	s.ScanKeys(context.Background(), scanner.ScanOptions{
		Pattern:   "test*",
		ScanCount: 10,
		Throttle:  10,
	})
}

func TestTencentCloudRedisService_GetKeysCount(t *testing.T) {
	redisAddr := "11.168.176.16:6379"
	c := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "Tencent88", // no password set
	})
	s := NewTencentCloudRedisService(c)
	r, e := s.GetKeysCount(context.Background())
	fmt.Println(r, e)
	assert.NoError(t, e)
}

func TestTencentCloudRedisService_GetMemoryUsage(t *testing.T) {
	redisAddr := "11.168.176.16:6379"
	c := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "Tencent88", // no password set
	})
	s := NewTencentCloudRedisService(c)
	r, e := s.GetMemoryUsage(context.Background(), scanner.KeyInfo{Key: "test", Node: "5225041744e3091d8499ffffbc44bf2a4afb898b"})
	fmt.Println(r, e)
	assert.NoError(t, e)
	r, e = s.GetMemoryUsage(context.Background(), scanner.KeyInfo{Key: "test", Node: "666832ea3b4ef9d7f83cca226406c08e7eea7c54"})
	fmt.Println(r, e)
	assert.Error(t, e)
}

func TestTencentCloudRedisService_GetKeyType(t *testing.T) {
	redisAddr := "11.168.176.16:6379"
	c := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "Tencent88", // no password set
		Username: "readonly",
	})
	s := NewTencentCloudRedisService(c)
	key := scanner.KeyInfo{Key: "test"}
	s.GetKeyType(context.Background(), &key)
	assert.Equal(t, "string", key.Type)
}

func TestTencentCloudRedisService_Batch(t *testing.T) {
	redisAddr := "11.168.176.16:6379"
	c := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "Tencent88", // no password set
	})
	s := NewTencentCloudRedisService(c)
	key1 := scanner.KeyInfo{Key: "test", Node: "5225041744e3091d8499ffffbc44bf2a4afb898b"}
	key2 := scanner.KeyInfo{Key: "test1", Node: "5225041744e3091d8499ffffbc44bf2a4afb898b"}
	keys := []*scanner.KeyInfo{&key1, &key2}
	s.GetTypeBatch(context.Background(), keys)
	assert.Equal(t, "string", key1.Type)
	s.GetMemoryUsageBatch(context.Background(), keys)
	assert.True(t, key1.BytesSize > 0)
	fmt.Printf("%+v\n%+v\n", key1, key2)
}
