package main

import (
	. "./scheduler"
	"fmt"
	"github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strconv"
)

func main() {
	if len(os.Args) != 6 {
		fmt.Println("Usage: go run tabu.go <submit_file> <cores> <dataSet=a b c d e> <round=1 2 3> <search_polic=local tabu>")
		os.Exit(1)
	}

	rand.Seed(1)

	submitFile := os.Args[1]
	cores, _ := strconv.Atoi(os.Args[2])
	dataSet := os.Args[3]
	round, _ := strconv.Atoi(os.Args[4])
	search_policy := os.Args[5]

	runtime.GOMAXPROCS(cores)

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, os.Kill) //todo: 让goroutine正常停止

	machines, instKV, appKV, machineKV := ReadData(dataSet)
	scheduler := NewScheduler(round, dataSet, submitFile, machines, instKV, appKV, machineKV)
	logrus.Infof("totalScore: %.8f\n", TotalScore(scheduler.InitSol.Machines))

	if search_policy == "tabu" {
		for i := 0; i < cores; i++ {
			go scheduler.TabuSearch()
		}
		<-stopChan
		scheduler.Output(dataSet)
		logrus.Infof("total score: %.6f\n", scheduler.BestSol.TotalScore)
	} else if search_policy == "local" {
		for i := 0; i < cores; i++ {
			go scheduler.LocalSearch()
		}
		<-stopChan
		//scheduler.LocalSearchOutput(dataSet)
		logrus.Infof("total score: %.6f\n", scheduler.BestSol.TotalScore)
	} else if search_policy == "analyse" {
		scheduler.Analyse()
	} else if search_policy == "movetolarge" {
		go scheduler.MoveToLargeMachine()
		<-stopChan
		scheduler.Output(dataSet)
		logrus.Infof("total score: %.6f\n", scheduler.BestSol.TotalScore)
	}
}
