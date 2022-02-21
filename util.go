package redmq

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/ntons/redis"
	"google.golang.org/protobuf/proto"
)

func getStreamKey(appId, topic string) string {
	return fmt.Sprintf("%s-%s", appId, topic)
}

//func getStreamCursors(
//	appId string, topicCursors []*v1pb.MsgQueTopicCursor) []string {
//	r := make([]string, 0, len(topicCursors)*2)
//	for _, a := range topicCursors {
//		r = append(r, getStreamKey(appId, a.Topic))
//	}
//	for _, a := range topicCursors {
//		r = append(r, a.Cursor)
//	}
//	return r
//}

func getTopicName(appId, streamKey string) string {
	return strings.TrimPrefix(streamKey, fmt.Sprintf("%s-", appId))
}

func encMsg(msg *Msg) interface{} {
	b, _ := proto.Marshal(msg)
	return []string{"b64val", base64.StdEncoding.EncodeToString(b)}
}

func decMsg(x redis.XMessage) (_ *Msg, err error) {
	v, ok := x.Values["b64val"]
	if !ok {
		return nil, fmt.Errorf("failed to get b64 val")
	}
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("bad val type: %T", v)
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("failed to decode b64 val")
	}
	m := &Msg{}
	if err = proto.Unmarshal(b, m); err != nil {
		return nil, fmt.Errorf("failed to unmarshal val")
	}
	m.Id = x.ID
	return m, nil
}

func msgIdLess(a, b string) bool {
	aa := strings.SplitN(a, "-", 2)
	bb := strings.SplitN(b, "-", 2)

	a0, _ := strconv.Atoi(aa[0])
	a1, _ := strconv.Atoi(aa[1])
	b0, _ := strconv.Atoi(bb[0])
	b1, _ := strconv.Atoi(bb[1])

	return a0 < b0 || (a0 == b0 && a1 < b1)
}
