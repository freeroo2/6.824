package raft

import (
	"fmt"
	"math/rand"
	"strings"
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

func EntriesToString(entries []*Entry) string {
	cmds := []string{}
	for _, e := range entries {
		cmds = append(cmds, fmt.Sprintf("%4d", e.Index))
	}
	return fmt.Sprint(strings.Join(cmds, "|"))
}
