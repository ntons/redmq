package redmq

const (
	// KEYS={
	//   1:Stream,
	// }
	// ARGV={
	//   1:Group,
	//   2:Consumer,
	//   3:MinIdleTime aka DeliveryTimeout,
	//   4:MaxDeadLetterQueueLen,
	//   5:MaxRetries,
	//   6:MaxClaims,
	// }
	//
	// XPENDING-1 O(1)      summary only
	// XPENDING-2 O(N)
	// XADD       O(1)
	// XACK       O(1)
	// XDEL       O(1)
	// XCLAIM     O(logN)
	luaRetryDeadLetter = `
local K, G, C, A3, A4, A5, A6 = KEYS[1], ARGV[1], ARGV[2], tonumber(ARGV[3]), tonumber(ARGV[4]), tonumber(ARGV[5]), tonumber(ARGV[6])
local function idless(a,b)
  local a1, a2 = tonumber(string.sub(a, 1, 13)), tonumber(string.sub(a, 15))
  local b1, b2 = tonumber(string.sub(b, 1, 13)), tonumber(string.sub(b, 15))
  return a1 < b1 or (a1 == b1 and a2 < b2)
end

local r = redis.call("XPENDING", K, G)
if r[1] == 0 then return {} end

local a = redis.call("XPENDING", K, G, r[2], r[3], r[1])
local x = redis.call("XINFO", "STREAM", K)[12][1]
local dlq, rty = {}, {}
for _,e in ipairs(a) do
  if e[3] > A3 then
    if idless(e[1], x) then
      redis.call("XACK", K, G, e[1])
    elseif e[4] > A5 then
      dlq[#dlq+1] = e[1]
    elseif #rty < A6 then
      rty[#rty+1] = e[1]
    end
  end
end

for _,e in ipairs(dlq) do
  redis.call("XADD", K..".dlq", "MAXLEN", "~", A4, "*", "ID", e, unpack(redis.call("XRANGE", K, e, e)[1][2]))
end

if #dlq > 0 then
  redis.call("XACK", K, G, unpack(dlq))
  redis.call("XDEL", K, unpack(dlq))
end

if #rty == 0 then
  return {}
else
  return redis.call("XCLAIM", K, G, C, A3, unpack(rty))
end
`

	// Remove some topics which has been idle(via meta topic-idle-since) for specified duration.
	// Remove some consumer group has been empty(via meta group-idle-since) for specified duration.
	// Remove some consumers has been idle(via XINFO CONSUMERS) for specified duration.
	luaShrink = `
local t = redis.call("TIME")
local t = math.floor(t[1]*1000 + t[2]/1000)
for _, K in ipairs(redis.call("KEYS", "redmq.topic.{*}")) do
  local m = cjson.decode(redis.call("HGET", K..".meta", "topic"))
  if t - redis.call("HGET",K..".meta","topic-shrink-at") > m["min-shrink-interval"] then
    redis.call("HSET", K..".meta", "topic-shrink-at", t)
    if (t - redis.call("HGET", K..".meta", "topic-idle-since")) > m["delete-idle-topic-after"] then
      redis.log(redis.LOG_NOTICE, "Shrink: Delete idle topic: "..K)
      redis.call("DEL", K, K..".dlq", K..".meta")
    else
      for _, g in ipairs(redis.call("XINFO", "GROUPS", K)) do
        local G = g[2]
        if t - redis.call("HGET", K..".meta", "group-"..G.."-idle-since") > m["delete-idle-group-after"] then
          redis.log(redis.LOG_NOTICE, "Shrink: Delete idle group: "..K.."."..G)
          redis.call("XGROUP", "DESTROY", K, G)
        else
          for _, c in ipairs(redis.call("XINFO", "CONSUMERS", K, G)) do
            local C = c[2]
            if c[6] > m["delete-idle-consumer-after"] then
              redis.log(redis.LOG_NOTICE, "Shrink: Delete idle consumer: "..C)
              redis.call("XGROUP", "DELCONSUMER", K, G, C)
            end      
          end      
        end
      end
    end
  end
end
return "OK"`
)
