package utils

import "strings"

// NameFromStreamARN takes a streamARN as input and returns the stream name
//
// return empty in place of error or stream name
func NameFromStreamARN(streamARN string) string {
	parts := strings.Split(streamARN, "/")
	if len(parts) == 2 {
		return parts[1]
	}

	return ""
}
