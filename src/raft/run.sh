#!/bin/bash

# go test -race -failfast -run SendHeartbeat -v
# go test -race -failfast -run TestUTStartElection -v -count 32
# go test -race -failfast -run TestUTRunFollower -v -count 16
# go test -race -failfast -run TestInitialElection2A  -v -count 4
go test -race -failfast -run TestReElection2A  -v -timeout 0 -count 409600 2>chan.out

