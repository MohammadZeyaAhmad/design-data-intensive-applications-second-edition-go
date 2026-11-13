package cache

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Decimal comparison avoids Lua's floating-point rounding above 2^53.
const compareVersion = `
local function less(a,b)
 if string.len(a)~=string.len(b) then return string.len(a)<string.len(b) end
 return a<b
end
local function clockms()
 local t=redis.call('TIME');return tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)
end
`

var getScript = redis.NewScript(compareVersion + `
local floor=redis.call('HGET',KEYS[1],'floor') or '0'
local version=redis.call('HGET',KEYS[1],'version') or '0'
local expires=tonumber(redis.call('HGET',KEYS[1],'expires') or '0')
local remaining=expires-clockms()
if remaining<=0 or redis.call('HSTRLEN',KEYS[1],'payload')>2048 then
 redis.call('HDEL',KEYS[1],'payload','expires');return {floor,version,'','0'}
end
return {floor,version,redis.call('HGET',KEYS[1],'payload') or '',tostring(remaining)}
`)
var putScript = redis.NewScript(compareVersion + `
local floor=redis.call('HGET',KEYS[1],'floor') or '0'
local previous=redis.call('HGET',KEYS[1],'version') or '0'
if less(ARGV[1],floor) or less(ARGV[1],previous) then return 0 end
redis.call('HSET',KEYS[1],'version',ARGV[1],'payload',ARGV[2],'expires',clockms()+tonumber(ARGV[3]))
redis.call('PEXPIRE',KEYS[1],ARGV[4]);return 1
`)
var invalidateScript = redis.NewScript(compareVersion + `
local floor=redis.call('HGET',KEYS[1],'floor') or '0'
if less(floor,ARGV[1]) then floor=ARGV[1];redis.call('HSET',KEYS[1],'floor',floor) end
local version=redis.call('HGET',KEYS[1],'version') or '0'
if less(version,floor) then redis.call('HDEL',KEYS[1],'payload','expires') end
redis.call('PEXPIRE',KEYS[1],ARGV[2]);return 1
`)

type Redis struct {
	Client *redis.Client
	prefix string
}

func NewRedis(address, password, prefix string) (*Redis, error) {
	host, port, e := net.SplitHostPort(address)
	n, pe := strconv.Atoi(port)
	if e != nil || pe != nil || host == "" || n < 1 || n > 65535 || strings.ContainsAny(address, " \r\n/@") || len(prefix) < 1 || len(prefix) > 100 || strings.ContainsAny(prefix, " \r\n") {
		return nil, errors.New("invalid Redis cache configuration")
	}
	client := redis.NewClient(&redis.Options{Addr: address, Password: password, MaxRetries: -1, DialTimeout: 100 * time.Millisecond, ReadTimeout: 80 * time.Millisecond, WriteTimeout: 80 * time.Millisecond, PoolTimeout: 50 * time.Millisecond, PoolSize: 8, MaxActiveConns: 8, MaxIdleConns: 4, ContextTimeoutEnabled: true})
	return &Redis{Client: client, prefix: prefix}, nil
}
func (r *Redis) name(key string) string { return r.prefix + ":" + key }
func (r *Redis) Get(ctx context.Context, key string) (Snapshot, error) {
	values, e := getScript.Run(ctx, r.Client, []string{r.name(key)}).Slice()
	if e != nil {
		return Snapshot{}, e
	}
	if len(values) != 4 {
		return Snapshot{}, ErrCache
	}
	text := make([]string, 4)
	for i, value := range values {
		v, ok := value.(string)
		if !ok {
			return Snapshot{}, ErrCache
		}
		text[i] = v
	}
	floor, e := strconv.ParseInt(text[0], 10, 64)
	if e != nil || floor < 0 {
		return Snapshot{}, ErrCache
	}
	version, e := strconv.ParseInt(text[1], 10, 64)
	if e != nil || version < 0 {
		return Snapshot{}, ErrCache
	}
	millis, e := strconv.ParseInt(text[3], 10, 64)
	if e != nil || millis < 0 || millis > 60000 {
		return Snapshot{}, ErrCache
	}
	return Snapshot{Floor: floor, Version: version, Payload: []byte(text[2]), Remaining: time.Duration(millis) * time.Millisecond}, nil
}
func (r *Redis) Put(ctx context.Context, key string, version int64, payload []byte, ttl, retention time.Duration) (bool, error) {
	if version < 0 || len(payload) > MaxValueBytes || ttl < time.Millisecond || retention < ttl {
		return false, ErrCache
	}
	n, e := putScript.Run(ctx, r.Client, []string{r.name(key)}, strconv.FormatInt(version, 10), payload, ttl.Milliseconds(), retention.Milliseconds()).Int()
	return n == 1, e
}
func (r *Redis) Invalidate(ctx context.Context, key string, version int64, retention time.Duration) error {
	if version < 0 || retention < time.Millisecond {
		return ErrCache
	}
	return invalidateScript.Run(ctx, r.Client, []string{r.name(key)}, strconv.FormatInt(version, 10), retention.Milliseconds()).Err()
}
func (r *Redis) Discard(ctx context.Context, key string) error {
	return r.Client.HDel(ctx, r.name(key), "payload", "expires").Err()
}
func (r *Redis) Close() error { return r.Client.Close() }
