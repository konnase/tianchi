#!/bin/bash

submit_file=${1:-"submit_localsearch_a.csv"}
cores=${2:-1}
dataset=${3:-"a"}
round=${4:-1}
method=${5:-"local"}

go run main.go $submit_file $cores $dataset $round $method