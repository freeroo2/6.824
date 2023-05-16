package raft

import (
	"math/rand"
	"time"
)

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func GetRand(server int64) int {
	rand.Seed(time.Now().Unix() + server)
	return rand.Intn(250)
}
