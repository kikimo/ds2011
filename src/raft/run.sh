#!/bin/bash

# go test -race -failfast -run SendHeartbeat -v
go test -race -failfast -run UT -v -count 2

