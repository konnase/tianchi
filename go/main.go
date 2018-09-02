package main

import (
	"os"
	"fmt"
	"math/rand"
	"strconv"
	"runtime"
	"os/signal"
	"github.com/sirupsen/logrus"
	. "./scheduler"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Println("Usage: go run tabu.go <submit_file> <cores> <dataset> <round>")
		os.Exit(1)
	}

	rand.Seed(1)

	submitFile := os.Args[1]
	cores, _ := strconv.Atoi(os.Args[2])
	dataset := os.Args[3]
	//round, _ := strconv.Atoi(os.Args[4])

	runtime.GOMAXPROCS(cores)

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, os.Kill) //todo: 让goroutine正常停止

	machines, instKV, appKV, machineKV := ReadData(dataset)
	scheduler := NewScheduler(dataset, submitFile, machines, instKV, appKV, machineKV)
	logrus.Infof("totalScore: %.8f\n",TotalScore(scheduler.InitSolution.Machines))

	for i := 0; i < cores; i++ {
		go scheduler.TabuSearch()
	}
	<-stopChan
	scheduler.Output(dataset)
	fmt.Printf("total score: %.6f\n", scheduler.BestSolution.TotalScore)
}
