package utils

import (
	"bytes"
	"math/rand"
)

func RandInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func RandIntn(n int) int {
	return rand.Intn(n)
}

func RandomString(l int) string {
	var result bytes.Buffer
	var temp string
	for i := 0; i < l; {
		if string(rune(RandInt(65, 90))) != temp {
			temp = string(rune(RandInt(65, 90)))
			result.WriteString(temp)
			i++
		}
	}
	return result.String()
}
