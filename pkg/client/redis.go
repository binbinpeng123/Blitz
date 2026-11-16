package client

import (
	"context"
	"fmt"
	"strings"

	"github.com/binbinpeng123/blitz/pkg/conf"
	"github.com/go-redis/redis/v7"
	"gitlab.hobot.cc/aidi/slardar/log"
)

const (
	batchZAddSize = 1000
)

type RedisClient struct {
	Client *redis.Client
}

func NewRedisClient() *RedisClient {
	var c *redis.Client
	cfg := conf.Conf.RedisConfig

	if cfg.MasterAddr != "" {
		opt := &redis.Options{
			Addr:       cfg.MasterAddr,
			Password:   cfg.Password,
			DB:         cfg.DB,
			MaxRetries: 3,
		}
		c = redis.NewClient(opt)
	}
	if cfg.SentinelAddr != "" {
		opt := &redis.FailoverOptions{
			MasterName:    cfg.MasterAddr,
			SentinelAddrs: strings.Split(cfg.SentinelAddr, ","),
			Password:      cfg.Password,
			DB:            cfg.DB,
			MaxRetries:    3,
		}
		c = redis.NewFailoverClient(opt)
	}

	// 测试连通性
	if err := c.Ping().Err(); err != nil {
		panic(fmt.Errorf("redis ping failed: %w", err))
	}

	return &RedisClient{Client: c}
}

func (r *RedisClient) ZSetAdd(ctx context.Context, key, member string, score float64) error {
	z := &redis.Z{
		Member: member,
		Score:  score,
	}
	return r.Client.ZAdd(key, z).Err()
}

func (r *RedisClient) ZSetRemove(ctx context.Context, key, member string) error {
	return r.Client.ZRem(key, member).Err()
}

func (r *RedisClient) ZSetMoveWithScore(ctx context.Context, from, to, member string, score float64) error {
	pipe := r.Client.TxPipeline()
	pipe.ZRem(from, member)
	pipe.ZAdd(to, &redis.Z{Score: score, Member: member})
	_, err := pipe.Exec()
	return err
}

func (r *RedisClient) ZSetTopN(ctx context.Context, key string, n int64) ([]redis.Z, error) {
	return r.Client.ZRevRangeWithScores(key, 0, n-1).Result()
}

func (r *RedisClient) SetIntValueByKey(ctx context.Context, key string, val int64) error {
	if err := r.Client.Set(key, val, 0).Err(); err != nil {
		log.Get(ctx).Errorf("SetIntValueByKey set failed: key=%s val=%d", key, val)
		return err
	}
	return nil
}

func (r *RedisClient) ZSetCount(ctx context.Context, key string) (int64, error) {
	return r.Client.ZCard(key).Result()
}

func (r *RedisClient) ZSetClearAllMembers(ctx context.Context, key string) error {
	return r.Client.Del(key).Err()
}

func (r *RedisClient) ZSetAddMulti(ctx context.Context, key string, items ...*redis.Z) error {
	for i := 0; i < len(items); i += batchZAddSize {
		end := i + batchZAddSize
		if end > len(items) {
			end = len(items)
		}
		batch := items[i:end]
		if err := r.Client.ZAdd(key, batch...).Err(); err != nil {
			log.Get(ctx).Errorf("ZSetAddMulti batch failed: key=%s i=%d end=%d", key, i, end)
			return err
		}
	}

	return nil
}
