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
local K,G,A2,A3,A4,A5,A6=KEYS[1],ARGV[1],ARGV[2],tonumber(ARGV[3]),tonumber(ARGV[4]),tonumber(ARGV[5]),tonumber(ARGV[6])
local r=redis.call("XPENDING",K,G)
local a=redis.call("XPENDING",K,G,r[2],r[3],r[1])
local dlq,rty={},{}
for _,e in ipairs(a) do if e[3]>A3 then if e[4]>A5 then dlq[#dlq+1]=e[1] elseif #rty<A6 then rty[#rty+1]=e[1] end end end
for _,e in ipairs(dlq) do redis.call("XADD",K..".dlq","MAXLEN","~",A4,"*","ID",e,unpack(redis.call("XRANGE",K,e,e)[1][2])) end
if #dlq>0 then redis.call("XACK",K,G,unpack(dlq)) redis.call("XDEL",K,unpack(dlq)) end
if #rty==0 then return {} end
return redis.call("XCLAIM",K,G,A2,A3,unpack(rty))
`

	// Create the group add set groupMeta
	luaCreateGroup = ``

	// Delete the consumer then update groupMeta if group empty
	luaDelConsumer = ``

	// Remove some topics which has been idle(via OBJECT IDLETIME) for specified duration.
	// Remove some consumers has been idle(via XINFO CONSUMERS) for specified duration.
	// Remove some consumer group has been empty(via meta empty-since) for specified duration.
	luaShrink = ``
)
