package utils

import (
	"strconv"
)

// TrimQuotes removes quotes from a string if they exist
func TrimQuotes(s string) string {
	if len(s) >= 2 {
		if c := s[len(s)-1]; s[0] == c && (c == '"' || c == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

func StringToInt64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

func Int64ToString(i int64) string {
	return strconv.FormatInt(i, 10)
}
