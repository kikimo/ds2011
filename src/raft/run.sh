#!/bin/bash

# go test -race -failfast -run SendHeartbeat -v
# go test -race -failfast -run TestUTStartElection -v -count 32
# go test -race -failfast -run TestUTRunFollower -v -count 16
go test -race -failfast -run TestInitialElection2A  -v -count 4

