package service

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HUSTtoKTH/redis-analyzer/src/scanner"
	"github.com/go-redis/redis/v8"
)

/**
腾讯云 redis 集群版, 很多 redis 命令不兼容, 需要加 node 后缀
**/

// NewTencentCloudRedisService creates RedisService
func NewTencentCloudRedisService(client *redis.Client) TencentCloudRedisService {
	// 连接获取集群分片信息
	var allNodes []string
	if clusterNodes, err := client.Do(context.Background(), "CLUSTER", "NODES").Result(); err == nil {
		str, ok := clusterNodes.(string)
		if ok {
			allNodes = extractClusterNodes(str)
		}
		fmt.Println("all nodes:", allNodes)
	} else {
		panic(fmt.Sprintf("Invalid cluster nodes, %s", err))
	}
	return TencentCloudRedisService{
		client:      client,
		masterNodes: allNodes,
	}
}

// TencentCloudRedisService  implementation for iteration over redis
type TencentCloudRedisService struct {
	client      *redis.Client
	masterNodes []string
}

// ScanKeys scans keys asynchroniously and sends them to the returned channel
func (s TencentCloudRedisService) ScanKeys(ctx context.Context, options scanner.ScanOptions) <-chan *scanner.KeyInfo {
	resultChan := make(chan *scanner.KeyInfo, 10000)
	var count string
	if options.ScanCount <= 0 {
		count = "1000"
	} else {
		count = fmt.Sprint(options.ScanCount)
	}
	go func() {
		defer close(resultChan)
		var wg sync.WaitGroup
		for i := 0; i < len(s.masterNodes); i++ {
			node := s.masterNodes[i]
			wg.Add(1)
			go func() {
				iter := "0"
				for {
					args := []interface{}{"scan", iter, "match", options.Pattern, "count", count, node}
					ans, err := s.client.Do(context.Background(), args...).Result()
					if err != nil {
						panic(fmt.Sprintf("error %v", err))
					}
					ansList, ok := ans.([]interface{})
					if !ok {
						panic(fmt.Sprintf("error %v", err))
					}
					iter, ok = ansList[0].(string)
					keys, ok2 := ansList[1].([]interface{})
					if !ok || !ok2 {
						panic(fmt.Sprintf("error %T %T", ansList[0], ansList[1]))
					}
					for _, key := range keys {
						resultChan <- &scanner.KeyInfo{
							Key:  fmt.Sprint(key),
							Node: node,
						}
						if options.Throttle > 0 {
							time.Sleep(time.Nanosecond * time.Duration(options.Throttle))
						}
					}
					i64, _ := strconv.ParseInt(iter, 10, 64)
					if i64 == 0 {
						break
					}
				}
				wg.Done()
			}()
		}
		wg.Wait()
	}()
	return resultChan
}

// GetKeysCount returns number of keys in the current database
func (s TencentCloudRedisService) GetKeysCount(ctx context.Context) (int64, error) {
	var keysCount int64
	var wg sync.WaitGroup
	for i := 0; i < len(s.masterNodes); i++ {
		node := s.masterNodes[i]
		wg.Add(1)
		go func() {
			result, err := s.client.Do(context.Background(), "DBSIZE", node).Result()
			if err != nil {
				panic(fmt.Sprintf("dbsize err %v,  nodes %v", err, node))
			}
			v, _ := result.(int64)
			atomic.AddInt64(&keysCount, v)
			wg.Done()
		}()
	}
	wg.Wait()
	return keysCount, nil
}

// GetMemoryUsage returns memory usage of given key
func (s TencentCloudRedisService) GetMemoryUsage(ctx context.Context, key scanner.KeyInfo) (int64, error) {
	var res int64
	result, err := s.client.Do(context.Background(), "MEMORY", "USAGE", key.Key, key.Node).Result()
	if err != nil {
		return res, errors.New(fmt.Sprintf("MEMORY USAGE err: %v, key: %v, node: %v", err, key.Key, key.Node))
	}
	res, ok := result.(int64)
	if !ok {
		return res, errors.New(fmt.Sprintf("res %T %v", result, result))
	}
	return res, nil
}

// GetKeyType TODO
func (s TencentCloudRedisService) GetKeyType(ctx context.Context, key *scanner.KeyInfo) {
	var res string
	result, err := s.client.Do(context.Background(), "TYPE", key.Key).Result()
	// fmt.Println(result, err)
	if err != nil {
		return
	}
	res, ok := result.(string)
	if !ok {
		return
	}
	key.Type = res
	return
}

// GetTypeBatch TODO
func (s TencentCloudRedisService) GetTypeBatch(ctx context.Context, keys []*scanner.KeyInfo) {
	m := []*redis.Cmd{}
	pipe := s.client.Pipeline()
	for _, key := range keys {
		m = append(m, pipe.Do(context.Background(), "TYPE", key.Key))
	}
	_, err := pipe.Exec(context.Background())
	if err != nil && err != redis.Nil {
		panic(err)
	}
	for i, v := range m {
		res, _ := v.Result()
		result, _ := res.(string)
		keys[i].Type = result
	}
}

// GetMemoryUsageBatch TODO
func (s TencentCloudRedisService) GetMemoryUsageBatch(ctx context.Context, keys []*scanner.KeyInfo) {
	m := []*redis.Cmd{}
	pipe := s.client.Pipeline()
	for _, key := range keys {
		m = append(m, pipe.Do(context.Background(), "MEMORY", "USAGE", key.Key, key.Node))
	}
	_, err := pipe.Exec(context.Background())
	if err != nil && err != redis.Nil {
		panic(err)
	}
	for i, v := range m {
		res, _ := v.Result()
		result, _ := res.(int64)
		keys[i].BytesSize = result
	}
}

// extractClusterNodes  获取集群的所有 master 节点
func extractClusterNodes(nodes string) (allNode []string) {
	allNode = make([]string, 0)
	lines := strings.Split(nodes, "\n")
	for _, line := range lines {
		// fmt.Printf("nodes: %s", line)
		if strings.Contains(line, "slave") {
			split := strings.Split(line, " ")
			if len(split) > 2 {
				allNode = append(allNode, split[0])
			}
		}
	}
	return allNode
}
