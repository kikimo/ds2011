#!/bin/bash

# go test -race -failfast -run SendHeartbeat -v
# go test -race -failfast -run TestUTStartElection -v -count 32
# go test -race -failfast -run TestUTRunFollower -v -count 16
# go test -race -failfast -run TestSnapshotRPC3B  -v -count 1 -timeout 20s
# go test -race -failfast -run TestSpeed3B -count 1 -timeout 0 -cpuprofile cpu.pprof
# go test -race -failfast -run TestSpeed3B -count 1 -timeout 0
# go test -race -failfast -run TestConcurrent3A  -v -count 128 -timeout 0 2>&1
# go test -race -failfast -run TestPersistPartition3A  -v -count 128 -timeout 0 2>&1
# go test -race -failfast -run TestReElection2A  -v -timeout 0 -count 409600 2>chan.out
# go test -race -failfast -run 2B -v -timeout 0
go test -race -failfast -run TestSnapshotRecoverManyClients3B -timeout 0
# go test -race -failfast -run TestSnapshotRecoverManyClients3B -timeout 0

