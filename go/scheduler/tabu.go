package scheduler

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"github.com/sirupsen/logrus"
)

type Scheduler struct {

	Neibors []*Candidate
	Candidates   []*Candidate

	InitSolution    *Solution
	UnchangedSolution *Solution  //在移动inst的过程中，不将inst从其原来的machine上删除
	BestSolution *Solution //固定位置solutions[0]
	CurrentSolution  *Solution //固定位置solutions[1]

	TabuList        map[ExchangeApp]int //禁忌表
	SearchResult map[string]string
	SubmitResult []SubmitResult
	PickFrom map[string]string

	Dataset string
	SubmitFile string
	Round int
	lock       sync.RWMutex
}

func NewScheduler(Dataset, SubmitFile string, machines []*Machine, instKV map[string]*Instance, appKV map[string]*Application, machineKV map[string]*Machine) *Scheduler {
	solution := &Solution{
		InstKV:      instKV,
		AppKV:       appKV,
		MachineKV:   machineKV,
		Machines:    machines,
	}
	scheduler := &Scheduler{
		InitSolution: solution,
		Dataset: Dataset,
		SubmitFile: SubmitFile,
		TabuList:   make(map[ExchangeApp]int),
		SearchResult: make(map[string]string),
		PickFrom: make(map[string]string),
	}
	scheduler.InitSolution.TotalScore = TotalScore(scheduler.InitSolution.Machines) //原始解的分数
	scheduler.InitSolution.PermitValue = scheduler.InitSolution.TotalScore
	scheduler.UnchangedSolution = GetNewSolutionFromCurrentSolution(scheduler.InitSolution)

	scheduler.Round = scheduler.readSubmitFile(SubmitFile)

	scheduler.BestSolution = GetNewSolutionFromCurrentSolution(scheduler.InitSolution)
	scheduler.CurrentSolution = GetNewSolutionFromCurrentSolution(scheduler.BestSolution)


	return scheduler
}

func (s *Scheduler) TabuSearch() {
	//for _, inst := range s.InitSolution.instKV {
	//	if inst.Id == "inst_39983" {
	//		machine := s.InitSolution.machineKV["machine_1688"]
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

	for round := 1; round <= 3; round ++ {
		if s.Round == -1 {
			round = 1
		}else{
			round = s.Round
		}
		var localBestCandidate *Candidate
		var localBestSolution *Solution
		logrus.Infof("round: %d", round)
		for {
			//得到候选集
			if getInitNeibor := s.getInitNeighbor(s.CurrentSolution); !getInitNeibor {
				s.prepareNextRound(s.CurrentSolution)
				break
			}
			s.getCandidateNeibor()
			//初始化候选集
			if len(s.TabuList) == 0 {
				for _, candidateX := range s.Candidates {
					candidateX.IsCandidate = true
				}
			} else {
				s.lock.Lock()
				for tabu := range s.TabuList {
					for _, candidateX := range s.Candidates {
						if tabu == candidateX.MoveAppFromMachineAToB {
							candidateX.IsCandidate = false
						} else {
							candidateX.IsCandidate = true
						}
						if candidateX.TotalScore < s.CurrentSolution.PermitValue { //特赦规则
							candidateX.IsCandidate = true
						}
					}
				}
				s.lock.Unlock()
			}
			//从候选集中选择分数最低的解
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
			//移动inst
			localBestSolution = s.CurrentSolution
			instA := localBestSolution.InstKV[localBestCandidate.InstA]
			uinstA := s.UnchangedSolution.InstKV[localBestCandidate.InstA]
			machineB := localBestSolution.MachineKV[localBestCandidate.MachineB]
			umachineB := s.UnchangedSolution.MachineKV[localBestCandidate.MachineB]
			//如果inst已经被移动了，则不再移动该inst
			if instA.Exchanged  {
				continue
			}
			ucanMove, _ := s.tryMove(uinstA, umachineB, s.UnchangedSolution, true)
			canMove, _ := s.tryMove(instA, machineB, localBestSolution, true)
			if  canMove && ucanMove && machineB.CanPutInst(instA) && umachineB.CanPutInst(uinstA){
				s.PickFrom[uinstA.Id] = uinstA.Machine.Id
				//logrus.Infof("%s can put %s: %t", machineB.Id, instA.Id, machineB.CanPutInst(instA))
				umachineB.NoCascatePut(uinstA)
				//todo: uinstA迁移之后居然对instA的迁移产生了影响：已经确定是复制solution之后，solution之间是有干扰的
				//logrus.Infof("%s can put %s: %t", machineB.Id, instA.Id, machineB.CanPutInst(instA))
				machineB.Put(instA)
				localBestSolution.TotalScore = localBestCandidate.TotalScore
				s.UnchangedSolution.TotalScore = localBestCandidate.TotalScore
				submitA := SubmitResult{round, instA.Id, instA.Machine.Id}
				s.SubmitResult = append(s.SubmitResult, submitA)

				//设置局部最优解的特赦值
				if localBestCandidate.TotalScore < s.CurrentSolution.PermitValue {
					localBestSolution.PermitValue = localBestCandidate.TotalScore
				} else if s.CurrentSolution.TotalScore < localBestCandidate.PermitValue {
					localBestSolution.PermitValue = s.CurrentSolution.TotalScore
				}
				//更新tabulist
				s.updateTabuList()
				s.lock.Lock()
				s.TabuList[localBestCandidate.MoveAppFromMachineAToB] = TabuLen

				//如果局部最优解优于当前最优解，则将局部最优解设置为当前最优解
				if localBestCandidate.TotalScore < s.BestSolution.TotalScore-0.0001 {
					logrus.Infof("New best solution: %0.8f --> %0.8f\n", s.BestSolution.TotalScore, localBestCandidate.TotalScore)
					s.BestSolution = nil
					s.BestSolution = GetNewSolutionFromCurrentSolution(localBestSolution)
				}

				s.CurrentSolution = localBestSolution
				logrus.Infof("Local best solution score: %.8f\n", localBestCandidate.TotalScore)
				//s.s.CurrentSolution = nil
				//s.s.CurrentSolution = s.GetNewSolutionFromCurrentSolution(localBestSolution)
				s.lock.Unlock()
			}
		}

	}
}

func (s *Scheduler) prepareNextRound(CurrentSolution *Solution) {
	for _, inst := range s.BestSolution.InstKV {
		inst.Exchanged = false
	}
	s.UnchangedSolution = nil
	s.UnchangedSolution = GetNewSolutionFromCurrentSolution(CurrentSolution)
	s.PickFrom = nil
}

func (s *Scheduler) updateTabuList() {
	s.lock.Lock()
	defer s.lock.Unlock()
	for key := range s.TabuList {
		s.TabuList[key]--
		if s.TabuList[key] == 0 {
			delete(s.TabuList, key)
		}
	}
}

func (s *Scheduler) getInitNeighbor(CurrentSolution *Solution) bool{
	failIter := 0
	for i := 0; i < InitNeiborSize; i++ {
		machineAIndex := rand.Intn(len(s.BestSolution.Machines))
		machineBIndex := s.getMachineBIndex()
		if machineAIndex == machineBIndex {
			i--
			continue
		}
		newNeibor := new(Candidate)
		newNeibor.TotalScore = 1e9
		s.Neibors = append(s.Neibors, newNeibor)
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
			for _, neibor := range s.Neibors{
				if neibor.MoveAppFromMachineAToB == MoveAppFromMachineAToB  {
					logrus.Infof("%s had been moved", MoveAppFromMachineAToB)
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			if canMove, totalScore := s.tryMove(instA, machineB, CurrentSolution, true); canMove { //交换第i个解里面随机的两个inst
				s.getNewNeibor(i, instA, machineA, machineB, totalScore)
				//fmt.Printf("move (%s) --> (%s): %f\n", instA.Id, machineB.Id, s.Neibors[i].TotalScore)
				moved = true
				break //产生一个邻居后，继续产生下一个邻居
			}
		}
		if !moved {
			failIter++
			i--
			s.Neibors = s.Neibors[:len(s.Neibors)-1]
		}
		if failIter > 1000 {
			logrus.Infof("failed to generate neighbor")
			return false
		}
	}
	return true
}

func (s *Scheduler) getMachineBIndex () int {
	machineBIndex := 0
	if s.Dataset == "c" || s.Dataset == "d"{
		rate := rand.Intn(100)
		if rate > 30 {
			machineBIndex = rand.Intn(3000) + 6000
		} else {
			machineBIndex = rand.Intn(6000)
		}
	} else if s.Dataset == "e" {
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

func (s *Scheduler) getNewNeibor(index int, instA *Instance, machineA, machineB *Machine, totalScore float64) {
	s.Neibors[index].InstA = instA.Id
	s.Neibors[index].MachineA = machineA.Id
	s.Neibors[index].MachineB = machineB.Id
	s.Neibors[index].TotalScore = totalScore
	s.Neibors[index].IsCandidate = false
	s.Neibors[index].PermitValue = totalScore
	s.Neibors[index].MoveAppFromMachineAToB = ExchangeApp{
		instA.AppId,
		machineA.Id,
		machineB.Id,
	}
}



func (s *Scheduler) getCandidateNeibor() {
	Neibors := NeiborSlice(s.Neibors[:])
	sort.Sort(Neibors)
	s.Candidates = Neibors[:CandidateLen]
	for _, candidate := range s.Candidates {
		candidate.IsCandidate = true
	}
}

func (s *Scheduler) PermitValue(nowBestSolution *Solution) float64 {
	return nowBestSolution.TotalScore
}

//SubmitFile: submit file;
func (s *Scheduler) readSubmitFile(SubmitFile string) int {
	if _, err := os.Open(SubmitFile); err!= nil {
		return -1
	}
	lines := ReadLines(SubmitFile)
	round := 1
	for _, line := range lines {
		split := strings.Split(line, ",")
		if inRound, _ := strconv.Atoi(split[0]); round == inRound{
			inst := s.InitSolution.InstKV[split[1]]
			machine := s.InitSolution.MachineKV[split[2]]
			s.PickFrom[inst.Id] = inst.Machine.Id
			machine.Put(inst)

			uinst := s.UnchangedSolution.InstKV[split[1]]
			umachine := s.UnchangedSolution.MachineKV[split[2]]
			//if uinst.Id == "inst_39983" {
			//	for _, instM := range umachine.InstKV {
			//		fmt.Printf("%s ", instM.App.Id)
			//	}
			//	fmt.Printf("\n")
			//	for appCnt, _ := range umachine.appCntKV {
			//		fmt.Printf("%s ", appCnt)
			//	}
			//	fmt.Printf("\n")
			//	logrus.Infof("%s can put %s : %t", umachine.Id, uinst.Id, umachine.CanPutInst(uinst))
			//	logrus.Infof("%s --> %s : %d", "app_2485", uinst.App.Id, umachine.appInterference["app_2485"][uinst.App.Id])
			//}
			umachine.NoCascatePut(uinst)

		} else{
			round++
			s.UnchangedSolution = nil
			s.UnchangedSolution = GetNewSolutionFromCurrentSolution(s.InitSolution)
			s.PickFrom = nil

			inst := s.InitSolution.InstKV[split[1]]
			machine := s.InitSolution.MachineKV[split[2]]
			s.PickFrom[inst.Id] = inst.Machine.Id
			machine.Put(inst)

			uinst := s.UnchangedSolution.InstKV[split[1]]
			umachine := s.UnchangedSolution.MachineKV[split[2]]
			umachine.NoCascatePut(uinst)
		}
	}
	s.InitSolution.TotalScore = TotalScore(s.InitSolution.Machines)
	s.InitSolution.PermitValue = s.InitSolution.TotalScore
	return round
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
	delta := score1 + score2 - (m1.Score() + m2.Score())

	if !force && (delta > 0 || -delta < 0.0001) {
		return false, 1e9
	}

	return true, solution.TotalScore + delta
}


func (s *Scheduler) Output(Dataset string) {

	filePath := fmt.Sprintf("submit_file_%s.csv", Dataset)
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, submit := range s.SubmitResult {
		line := fmt.Sprintf("%d,%s,%s", submit.Round, submit.Instance, submit.Machine)
		//line := fmt.Sprintf("%s,%s", submit.instance, submit.machine)
		fmt.Fprintln(w, line)
	}
	w.Flush()
	fmt.Printf("writing to %s\n", filePath)
}


