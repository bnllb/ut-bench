package data_sync

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/mennanov/limiters"
	"github.com/redis/go-redis/v9"
)

type ApiId string

const (
	redisKeyPrefix = "apiRateLimiter:leaky:"
)

type ApiRateLimiter struct {
	rlMapLock sync.RWMutex
	rlMap     map[ApiId]*limiters.LeakyBucket // 每个 api 的限制

	defaultQps       int
	rdb              *redis.Client
	redisKeyPrefixL2 string
}

// TODO: 支持配置每个api的qps限制
func NewApiRateLimiter(rdb *redis.Client, aliuid, idpId int64, defaultQps int) *ApiRateLimiter {
	return &ApiRateLimiter{
		rlMapLock:        sync.RWMutex{},
		rlMap:            map[ApiId]*limiters.LeakyBucket{},
		defaultQps:       defaultQps,
		rdb:              rdb,
		redisKeyPrefixL2: fmt.Sprintf("%s:%d:%d:", redisKeyPrefix, aliuid, idpId),
	}
}

func (is *ApiRateLimiter) Wait(ctx context.Context, key ApiId) error {

	is.rlMapLock.RLock()
	rl, ok := is.rlMap[key]
	is.rlMapLock.RUnlock()
	if !ok {
		// rl = rate.NewLimiter(rate.Limit(is.defaultQps), is.defaultQps) // TODO: 控制台配置身份源时也需要调用 api，存在限流问题
		prefixL3 := is.redisKeyPrefixL2 + string(key) + ":"
		rate := time.Duration(1e9 / is.defaultQps)
		rl = limiters.NewLeakyBucket(
			int64(is.defaultQps),
			rate,
			limiters.NewLockRedis(goredis.NewPool(is.rdb), prefixL3+"lock"),
			limiters.NewLeakyBucketRedis(is.rdb, prefixL3+"state", rate, false),
			limiters.NewSystemClock(),
			limiters.NewStdLogger(),
		)
		is.rlMapLock.Lock()
		is.rlMap[key] = rl
		is.rlMapLock.Unlock()
	}
	wait, err := rl.Limit(ctx)
	if err != nil {
		// 预期此处不应该出现错误，如果出现错误，说明限流器出现问题，兜底解决方案改为等待1-3s
		// lg.LfsFromContext(ctx).Errorf("LeakyBucket.Limit error: %v", err)
		wait := time.Millisecond * time.Duration(1000+rand.Int()%2000)
		time.Sleep(wait)
	} else {
		time.Sleep(wait)
	}

	return nil
}
