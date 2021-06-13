package raft

import "sync"

type config struct {
	sync.Mutex
}
