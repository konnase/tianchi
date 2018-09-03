package scheduler

import (
	"bufio"
	"fmt"
	"github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
)

type Scheduler struct {
	Neighbors  []*Candidate
	Candidates []*Candidate

	InitSol      *Solution
	UnchangedSol *Solution //在移动inst的过程中，不将inst从其原来的machine上删除
	BestSol      *Solution //固定位置solutions[0]
	CurrentSol   *Solution //固定位置solutions[1]

	TabuKv        map[ExchangeApp]int //禁忌表
	SearchResult  map[string]string
	SubmitResult  []SubmitResult
	PendingInstKv map[string]string

	DataSet    string
	SubmitFile string
	Round      int
}

func NewScheduler(round int, dataSet, SubmitFile string, machines []*Machine, instKV map[string]*Instance, appKV map[string]*Application, machineKV map[string]*Machine) *Scheduler {
	solution := &Solution{
		InstKV:    instKV,
		AppKV:     appKV,
		MachineKV: machineKV,
		Machines:  machines,
	}
	sch := &Scheduler{
		InitSol:       solution,
		DataSet:       dataSet,
		SubmitFile:    SubmitFile,
		Round:         round,
		TabuKv:        make(map[ExchangeApp]int),
		SearchResult:  make(map[string]string),
		PendingInstKv: make(map[string]string),
	}
	sch.InitSol.TotalScore = TotalScore(sch.InitSol.Machines) //原始解的分数
	sch.InitSol.PermitValue = sch.InitSol.TotalScore
	sch.UnchangedSol = CopySolution(sch.InitSol)

	sch.readSubmitFile(SubmitFile)

	sch.BestSol = CopySolution(sch.InitSol)
	sch.CurrentSol = CopySolution(sch.BestSol)

	return sch
}

//SubmitFile: submit file;
func (s *Scheduler) readSubmitFile(SubmitFile string) {
	if _, err := os.Open(SubmitFile); err != nil {
		return
	}
	lines := ReadLines(SubmitFile)
	round := 1
	for _, line := range lines {
		split := strings.Split(line, ",")
		if inRound, _ := strconv.Atoi(split[0]); round == inRound {
			s.moveInstViaSubmitFile(split[1], split[2])
		} else {
			round++
			s.prepareNextRound()
			s.moveInstViaSubmitFile(split[1], split[2])
		}
	}
	s.InitSol.TotalScore = TotalScore(s.InitSol.Machines)
	s.InitSol.PermitValue = s.InitSol.TotalScore
}

func (s *Scheduler) prepareNextRound() {
	for instId := range s.PendingInstKv {
		inst := s.InitSol.InstKV[instId]
		inst.Exchanged = false
	}
	s.UnchangedSol = nil
	s.UnchangedSol = CopySolution(s.InitSol)
	s.PendingInstKv = nil
}

func (s *Scheduler) moveInstViaSubmitFile(instId, machineId string) {
	inst := s.InitSol.InstKV[instId]
	machine := s.InitSol.MachineKV[machineId]
	s.PendingInstKv[inst.Id] = inst.Machine.Id
	machine.Put(inst, true)

	uinst := s.UnchangedSol.InstKV[instId]
	umachine := s.UnchangedSol.MachineKV[machineId]
	umachine.Put(uinst, false)
}

func (s *Scheduler) TabuSearch() {
	//for _, inst := range s.InitSol.instKV {
	//	if inst.Id == "inst_39983" {
	//		machine := s.InitSol.machineKV["machine_1688"]
	//		for _, instM := range machine.instKV {
	//			fmt.Printf("%s ",instM.App.Id)
	//		}
	//		logrus.Infof("%s can put %s : %t", machine.Id, inst.Id, machine.CanPutInst(inst))
	//		logrus.Infof("%s --> %s : %d", inst.App.Id, "app_1310", machine.appInterference[inst.App.Id]["app_1310"])
	//	}
	//}
	s.StartSearch()
}

func (s *Scheduler) StartSearch() {
	round := s.Round
	var localBestCandidate *Candidate
	var localBestSolution *Solution
	logrus.Infof("round: %d", round)
	for {
		//得到候选集
		if !s.getInitNeighbor(s.CurrentSol) {
			continue
		}
		s.getCandidateNeighbor()
		//初始化候选集
		s.initCandidateSet()
		//从候选集中选择分数最低的解
		localBestCandidate = s.getMinScoreSolution()
		//移动inst
		localBestSolution = s.CurrentSol
		instA := localBestSolution.InstKV[localBestCandidate.InstA]
		uinstA := s.UnchangedSol.InstKV[localBestCandidate.InstA]
		machineB := localBestSolution.MachineKV[localBestCandidate.MachineB]
		umachineB := s.UnchangedSol.MachineKV[localBestCandidate.MachineB]
		//如果inst已经被移动了，则不再移动该inst
		if instA.Exchanged {
			continue
		}
		ucanMove, _ := s.tryMove(uinstA, umachineB, s.UnchangedSol, false)
		canMove, _ := s.tryMove(instA, machineB, localBestSolution, false)
		if canMove && ucanMove {
			//todo: uinstA迁移之后居然对instA的迁移产生了影响：已经确定是复制solution之后，solution之间是有干扰的
			//todo: 是复制solution的时候即GetNewSolutionFromCurrentSolution()方法里面machine.appCntKV没有重新创建
			s.PendingInstKv[uinstA.Id] = uinstA.Machine.Id
			umachineB.Put(uinstA, false)
			machineB.Put(instA, true)

			localBestSolution.TotalScore = localBestCandidate.TotalScore
			s.UnchangedSol.TotalScore = localBestCandidate.TotalScore

			submitA := SubmitResult{round, instA.Id, instA.Machine.Id}
			s.SubmitResult = append(s.SubmitResult, submitA)

			//设置局部最优解的特赦值
			if localBestCandidate.TotalScore < s.CurrentSol.PermitValue {
				localBestSolution.PermitValue = localBestCandidate.TotalScore
			} else if s.CurrentSol.TotalScore < localBestCandidate.PermitValue {
				localBestSolution.PermitValue = s.CurrentSol.TotalScore
			}
			//更新tabulist
			s.updateTabuList()
			s.TabuKv[localBestCandidate.MoveApp] = TabuLen

			//如果局部最优解优于当前最优解，则将局部最优解设置为当前最优解
			if localBestCandidate.TotalScore < s.BestSol.TotalScore-0.0001 {
				logrus.Infof("New best solution: %0.8f --> %0.8f\n", s.BestSol.TotalScore, localBestCandidate.TotalScore)
				s.BestSol = nil
				s.BestSol = CopySolution(localBestSolution)
			}

			s.CurrentSol = localBestSolution
			logrus.Infof("Local best solution score: %.8f\n", localBestCandidate.TotalScore)
		}
	}

}

func (s *Scheduler) initCandidateSet() {
	//初始化候选集
	if len(s.TabuKv) == 0 {
		for _, candidateX := range s.Candidates {
			candidateX.IsCandidate = true
		}
	} else {
		for tabu := range s.TabuKv {
			for _, candidateX := range s.Candidates {
				if tabu == candidateX.MoveApp {
					candidateX.IsCandidate = false
				} else {
					candidateX.IsCandidate = true
				}
				if candidateX.TotalScore < s.CurrentSol.PermitValue { //特赦规则
					candidateX.IsCandidate = true
				}
			}
		}
	}
}

func (s *Scheduler) getMinScoreSolution() *Candidate {
	var localBestCandidate *Candidate
	i := 0
	for _, candidateS := range s.Candidates {
		// println(candidateS.TotalScore)
		if candidateS.IsCandidate {
			if i == 0 {
				localBestCandidate = candidateS
			}
			if candidateS.TotalScore < localBestCandidate.TotalScore {
				localBestCandidate = candidateS
			}
			i++
		}
	}
	return localBestCandidate
}

func (s *Scheduler) updateTabuList() {
	for key := range s.TabuKv {
		s.TabuKv[key]--
		if s.TabuKv[key] == 0 {
			delete(s.TabuKv, key)
		}
	}
}

func (s *Scheduler) getInitNeighbor(CurrentSolution *Solution) bool {
	s.Neighbors = s.Neighbors[:0:0]
	failIter := 0
	for i := 0; i < InitNeighborSize; i++ {
		machineAIndex := rand.Intn(len(s.BestSol.Machines))
		machineBIndex := s.getMachineBIndex()
		if machineAIndex == machineBIndex {
			i--
			continue
		}
		newNeighbor := new(Candidate)
		newNeighbor.TotalScore = 1e9
		s.Neighbors = append(s.Neighbors, newNeighbor)
		moved := false
		machineA := CurrentSolution.Machines[machineAIndex]
		machineB := CurrentSolution.Machines[machineBIndex]
		for _, instA := range machineA.AppKV {
			//如果machineA上的appA已经迁移到machineB了，则重新生成
			MoveAppFromMachineAToB := ExchangeApp{
				instA.AppId,
				machineA.Id,
				CurrentSolution.Machines[machineBIndex].Id,
			}
			duplicate := false
			for _, Neighbor := range s.Neighbors {
				if Neighbor.MoveApp == MoveAppFromMachineAToB {
					logrus.Infof("%s had been moved", MoveAppFromMachineAToB)
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			canMove, totalScore := s.tryMove(instA, machineB, CurrentSolution, false)
			uinstA := s.UnchangedSol.InstKV[instA.Id]
			umachineB := s.UnchangedSol.Machines[machineBIndex]
			ucanMove, _ := s.tryMove(uinstA, umachineB, s.UnchangedSol, false)
			if canMove && ucanMove {
				s.getNewNeighbor(i, instA, machineA, machineB, totalScore)
				moved = true
				break //产生一个邻居后，继续产生下一个邻居
			}
		}
		if !moved {
			failIter++
			i--
			s.Neighbors = s.Neighbors[:len(s.Neighbors)-1]
		}
		if failIter > 1000 {
			//logrus.Infof("failed to generate neighbor")
			return false
		}
	}
	return true
}

func (s *Scheduler) getMachineBIndex() int {
	machineBIndex := 0
	if s.DataSet == "c" || s.DataSet == "d" {
		rate := rand.Intn(100)
		if rate > 30 {
			machineBIndex = rand.Intn(3000) + 6000
		} else {
			machineBIndex = rand.Intn(6000)
		}
	} else if s.DataSet == "e" {
		rate := rand.Intn(100)
		if rate > 50 {
			machineBIndex = rand.Intn(2000) + 6000
		} else {
			machineBIndex = rand.Intn(6000)
		}
	} else {
		//a, b的分数都在5000以下，故目标机器的范围不需要太大
		machineBIndex = rand.Intn(5000)
	}
	return machineBIndex
}

func (s *Scheduler) getNewNeighbor(index int, instA *Instance, machineA, machineB *Machine, totalScore float64) {
	s.Neighbors[index].InstA = instA.Id
	s.Neighbors[index].MachineA = machineA.Id
	s.Neighbors[index].MachineB = machineB.Id
	s.Neighbors[index].TotalScore = totalScore
	s.Neighbors[index].IsCandidate = false
	s.Neighbors[index].PermitValue = totalScore
	s.Neighbors[index].MoveApp = ExchangeApp{
		instA.AppId,
		machineA.Id,
		machineB.Id,
	}
}

func (s *Scheduler) getCandidateNeighbor() {
	Neighbors := NeighborSlice(s.Neighbors[:])
	sort.Sort(Neighbors)
	s.Candidates = Neighbors[:CandidateLen]
	for _, candidate := range s.Candidates {
		candidate.IsCandidate = true
	}
}

func (s *Scheduler) PermitValue(nowBestSolution *Solution) float64 {
	return nowBestSolution.TotalScore
}

func (s *Scheduler) tryMove(inst *Instance, m2 *Machine, solution *Solution, force bool) (bool, float64) {
	if inst.Exchanged {
		return false, 1e9
	}
	m1 := inst.Machine
	if m1.Id == m2.Id {
		return false, 1e9
	}
	m1.Lock.Lock()
	m2.Lock.Lock()
	defer m2.Lock.Unlock()
	defer m1.Lock.Unlock()
	if !m2.CanPutInst(inst) {
		return false, 1e9
	}

	var cpu1 [98]float64
	var cpu2 [98]float64
	machineCpu1 := m1.CpuUsage()
	machineCpu2 := m2.CpuUsage()
	for i := 0; i < 98; i++ {
		cpu1[i] = machineCpu1[i] - inst.App.Cpu[i]
		cpu2[i] = machineCpu2[i] + inst.App.Cpu[i]
	}
	score1 := CpuScore(cpu1[0:98], m1.CpuCapacity, len(m1.InstKV))
	score2 := CpuScore(cpu2[0:98], m2.CpuCapacity, len(m2.InstKV))
	delta := score1 + score2 - (m1.GetScore() + m2.GetScore())

	if !force && (delta > 0 || -delta < 0.0001) {
		return false, 1e9
	}

	return true, solution.TotalScore + delta
}

func (s *Scheduler) Output(dataSet string) {

	filePath := fmt.Sprintf("submit_file_%s.csv", dataSet)
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
