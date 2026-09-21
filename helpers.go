package broker

import (
	"fmt"
	"strconv"
)

func itoa(v int) string { return strconv.Itoa(v) }

func itoa64(v int64) string { return strconv.FormatInt(v, 10) }

func cloneByteMap(in map[string][]byte) map[string][]byte {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]byte, len(in))
	for k, v := range in {
		out[k] = append([]byte(nil), v...)
	}
	return out
}

func stringifyAny(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}
