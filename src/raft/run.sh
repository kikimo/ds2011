#!/bin/bash

# go test -race -failfast -run SendHeartbeat -v
go test -race -failfast -run TestAppendEntries -v

