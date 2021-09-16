#!/bin/bash

# go test -race -failfast -run SendHeartbeat -v
# go test -race -failfast -run TestUTStartElection -v -count 32
# go test -race -failfast -run TestUTRunFollower -v -count 16
# go test -race -failfast -run TestInitialElection2A  -v -count 4
# go test -race -failfast -run TestInitialElection2A  -v -timeout 0 -count 1024 2>chan.out
# go test -race -failfast -run TestFailAgree2B -v -timeout 0 -count 2 2>chan.out
# go test -race -failfast -run TestFailAgree2B -v -timeout 0 -count 2 2>chan.out

# go test -race -failfast -run UT  -v -timeout 0 -count 2 2>chan.out
# go test -race -failfast -run 2A  -v -timeout 0 -count 2 2>chan.out
# go test -race -failfast -run 2B -v -timeout 0 -count 2 2>chan.out
# go test -race -failfast -run 2C -v -timeout 0 -count 2 2>chan.out

go test -race -failfast -run TestSnapshotInstallUnreliable2D -v -timeout 0 -count 32 -memprofile mem.out | tee TestSnapshotInstallUnreliable2D.leak.log
# go test -race -failfast -run TestSnapshotInstallUnreliable2D -v -timeout 0 -count 8

