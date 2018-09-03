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
	if len(os.Args) != 5 {
		fmt.Println("Usage: go run tabu.go <submit_file> <cores> <dataSet> <round>")
		os.Exit(1)
	}

	rand.Seed(1)

	submitFile := os.Args[1]
	cores, _ := strconv.Atoi(os.Args[2])
	dataSet := os.Args[3]
	round, _ := strconv.Atoi(os.Args[4])

	runtime.GOMAXPROCS(cores)

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, os.Kill) //todo: 让goroutine正常停止

	machines, instKV, appKV, machineKV := ReadData(dataSet)
	scheduler := NewScheduler(round, dataSet, submitFile, machines, instKV, appKV, machineKV)
	logrus.Infof("totalScore: %.8f\n", TotalScore(scheduler.InitSol.Machines))

	for i := 0; i < cores; i++ {
		go scheduler.TabuSearch()
	}
	<-stopChan
	scheduler.Output(dataSet)
	logrus.Infof("total score: %.6f\n", scheduler.BestSol.TotalScore)
}
