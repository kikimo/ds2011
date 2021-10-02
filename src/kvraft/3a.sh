#!/bin/bash

# go test -race -failfast -run SendHeartbeat -v
# go test -race -failfast -run TestUTStartElection -v -count 32
# go test -race -failfast -run TestUTRunFollower -v -count 16
go test -race -failfast -run 3A -count 1 -timeout 0
# go test -race -failfast -run "TestSpeed3A|TestConcurrent3A" -count 1 -timeout 0
# go test -race -failfast -run "TestConcurrent3A" -count 1 -timeout 0
# go test -race -failfast -run TestConcurrent3A  -v -count 128 -timeout 0 2>&1
# go test -race -failfast -run TestPersistPartition3A  -v -count 128 -timeout 0 2>&1
# go test -race -failfast -run TestReElection2A  -v -timeout 0 -count 409600 2>chan.out
# go test -race -failfast -run 2B -v -timeout 0

