package scheduler

import (
	"github.com/sirupsen/logrus"
	"os"
	"math/rand"
	"fmt"
	"bufio"
)

func (s *Scheduler) LocalSearch() {
	if s.SubmitRound < s.Round {
		s.prepareNextRound()
		logrus.Infof("Start next round...")
	} else if s.SubmitRound > s.Round {
		logrus.Infof("invalid round! ")
		os.Exit(0)
	}

	s.BestSol = CopySolution(s.InitSol)
	s.CurrentSol = CopySolution(s.BestSol)

	round := s.Round
	iter := 0
	var localBestSolution *Solution

	logrus.Infof("CurrentSol score: %f", TotalScore(s.BestSol.Machines))
	logrus.Infof("round: %d", round)
	for {
		iter++
		for i := 0; i < InitNeighborSize; i++ {
			machineAIndex := rand.Intn(len(s.BestSol.Machines))
			machineBIndex := s.getMachineBIndex() //优化：针对不同的数据集使用不同的策略产生目标机器：目标机器为大机器的概率更大
			if machineAIndex == machineBIndex {
				continue
			}

			newNeighbor := new(Candidate) //优化：只记录邻居相对currentSolution的单步移动，避免记录整个solution导致内存开销过大
			newNeighbor.TotalScore = 1e9
			s.Neighbors = append(s.Neighbors, newNeighbor)

			machineA := s.CurrentSol.Machines[machineAIndex]
			machineB := s.CurrentSol.Machines[machineBIndex]

			moved := false
			for _, instA := range machineA.AppKV {
				canMove, totalScore := s.tryMove(instA, machineB, s.CurrentSol, false)
				uinstA := s.UnchangedSol.InstKV[instA.Id]
				umachineB := s.UnchangedSol.Machines[machineBIndex]
				ucanMove, _ := s.tryMove(uinstA, umachineB, s.UnchangedSol, false)
				if canMove && ucanMove {
					//移动inst
					localBestSolution = s.CurrentSol
					umachineB.Put(uinstA, false)
					machineB.Put(instA, true)

					localBestSolution.TotalScore = totalScore
					s.UnchangedSol.TotalScore = totalScore

					submitA := SubmitResult{round, instA.Id, instA.Machine.Id, machineA.Id}
					s.SubmitResult = append(s.SubmitResult, submitA)

					//如果局部最优解优于当前最优解，则将局部最优解设置为当前最优解
					if totalScore < s.BestSol.TotalScore-0.0001 {
						logrus.Infof("New best solution: %0.8f --> %0.8f\n", s.BestSol.TotalScore, totalScore)
						s.BestSol = nil
						s.BestSol = CopySolution(localBestSolution)
					}

					s.CurrentSol = localBestSolution
					logrus.Infof("Local best solution score: %.8f\n", totalScore)

					s.backSpace(iter)
					moved = true
					break //产生一个邻居后，继续产生下一个邻居
				}
			}
			if moved {
				break
			}
		}

	}
}

func (s *Scheduler) LocalSearchOutput(dataSet string) {

	filePath := fmt.Sprintf("submit_localsearch_%s.csv", dataSet)
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, submit := range s.SubmitResult {
		line := fmt.Sprintf("%d,%s,%s", submit.Round, submit.Instance, submit.Machine)
		fmt.Fprintln(w, line)
	}
	w.Flush()
	logrus.Infof("writing to %s\n", filePath)
}