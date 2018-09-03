package scheduler

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"github.com/sirupsen/logrus"
)

type Scheduler struct {

	Neibors []*Candidate
	Candidates   []*Candidate

	InitSolution    *Solution
	UnchangedSolution *Solution  //在移动inst的过程中，不将inst从其原来的machine上删除
	BestSolution *Solution
	CurrentSolution  *Solution

	TabuList        map[ExchangeApp]int //禁忌表
	SubmitResult []SubmitResult
	PickFrom map[string]string  //记录inst原来部署的机器的machineId

	Dataset string
	SubmitFile string
	Round int
	SubmitRound int
}

func NewScheduler(round int, Dataset, SubmitFile string, machines []*Machine, instKV map[string]*Instance, appKV map[string]*Application, machineKV map[string]*Machine) *Scheduler {
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
		Round:round,
		TabuList:   make(map[ExchangeApp]int),
		PickFrom: make(map[string]string),
	}
	scheduler.InitSolution.TotalScore = TotalScore(scheduler.InitSolution.Machines) //原始解的分数
	scheduler.InitSolution.PermitValue = scheduler.InitSolution.TotalScore
	scheduler.UnchangedSolution = GetNewSolutionFromCurrentSolution(scheduler.InitSolution)

	scheduler.SubmitRound = scheduler.readSubmitFile(SubmitFile)  //优化：从submitFile中读取inst迁移过程，节省迁移搜索时间

	return scheduler
}

//SubmitFile: submit file;
func (s *Scheduler) readSubmitFile(SubmitFile string) int{
	if _, err := os.Open(SubmitFile); err!= nil {
		return -1
	}
	lines := ReadLines(SubmitFile)
	round := 1
	for _, line := range lines {
		split := strings.Split(line, ",")
		if inRound, _ := strconv.Atoi(split[0]); round == inRound{
			s.moveInstViaSubmitFile(split[1], split[2])
		} else{
			round++
			s.prepareNextRound()
			s.moveInstViaSubmitFile(split[1], split[2])
		}
	}
	s.InitSolution.TotalScore = TotalScore(s.InitSolution.Machines)
	s.InitSolution.PermitValue = s.InitSolution.TotalScore
	return round
}

func (s *Scheduler) prepareNextRound() {
	for instId := range s.PickFrom {
		inst := s.InitSolution.InstKV[instId]
		inst.Exchanged = false
	}
	s.UnchangedSolution = nil
	s.UnchangedSolution = GetNewSolutionFromCurrentSolution(s.InitSolution)
	s.PickFrom = nil
	s.PickFrom = make(map[string]string)
}

func (s *Scheduler) moveInstViaSubmitFile(instId, machineId string) {
	inst := s.InitSolution.InstKV[instId]
	machine := s.InitSolution.MachineKV[machineId]
	s.PickFrom[inst.Id] = inst.Machine.Id
	machine.Put(inst)

	uinst := s.UnchangedSolution.InstKV[instId]
	umachine := s.UnchangedSolution.MachineKV[machineId]
	umachine.NoCascatePut(uinst)
}

func (s *Scheduler) TabuSearch() {
	s.StartSearch()
}

func (s *Scheduler) StartSearch() {
	if s.SubmitRound < s.Round {
		s.prepareNextRound()
		logrus.Infof("Start next round...")
	}
	s.BestSolution = GetNewSolutionFromCurrentSolution(s.InitSolution)
	s.CurrentSolution = GetNewSolutionFromCurrentSolution(s.BestSolution)
	round := s.Round
	var localBestCandidate *Candidate
	var localBestSolution *Solution
	logrus.Infof("round: %d", round)
	for {
		//得到候选集
		if !s.getInitNeighbor(s.CurrentSolution) { continue }
		s.getCandidateNeibor()
		//初始化候选集
		s.initCandidateSet()
		//从候选集中选择分数最低的解
		localBestCandidate = s.getMinScoreSolution()
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
		ucanMove, _ := s.tryMove(uinstA, umachineB, s.UnchangedSolution, false)
		canMove, _ := s.tryMove(instA, machineB, localBestSolution, false)
		if  canMove && ucanMove {
			//todo: uinstA迁移之后居然对instA的迁移产生了影响：已经确定是复制solution之后，solution之间是有干扰的
			//todo: 是复制solution的时候即GetNewSolutionFromCurrentSolution()方法里面machine.appCntKV没有重新创建
			s.PickFrom[uinstA.Id] = uinstA.Machine.Id
			umachineB.NoCascatePut(uinstA)
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
			s.TabuList[localBestCandidate.MoveAppFromMachineAToB] = TabuLen

			//如果局部最优解优于当前最优解，则将局部最优解设置为当前最优解
			if localBestCandidate.TotalScore < s.BestSolution.TotalScore-0.0001 {
				logrus.Infof("New best solution: %0.8f --> %0.8f\n", s.BestSolution.TotalScore, localBestCandidate.TotalScore)
				s.BestSolution = nil
				s.BestSolution = GetNewSolutionFromCurrentSolution(localBestSolution)
			}

			s.CurrentSolution = localBestSolution
			logrus.Infof("Local best solution score: %.8f\n", localBestCandidate.TotalScore)
		}
	}

}

func (s *Scheduler) initCandidateSet () {
	//初始化候选集
	if len(s.TabuList) == 0 {
		for _, candidateX := range s.Candidates {
			candidateX.IsCandidate = true
		}
	} else {
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
	for key := range s.TabuList {
		s.TabuList[key]--
		if s.TabuList[key] == 0 {
			delete(s.TabuList, key)
		}
	}
}

func (s *Scheduler) getInitNeighbor(CurrentSolution *Solution) bool{
	s.Neibors = s.Neibors[:0:0] //优化：生成neighbors之前先清空，避免neighbors臃肿
	failIter := 0
	for i := 0; i < InitNeiborSize; i++ {
		machineAIndex := rand.Intn(len(s.BestSolution.Machines))
		machineBIndex := s.getMachineBIndex()  //优化：针对不同的数据集使用不同的策略产生目标机器：目标机器为大机器的概率更大
		if machineAIndex == machineBIndex {
			i--
			continue
		}
		newNeibor := new(Candidate)  //优化：只记录邻居相对currentSolution的单步移动，避免记录整个solution导致内存开销过大
		newNeibor.TotalScore = 1e9
		s.Neibors = append(s.Neibors, newNeibor)
		moved := false
		machineA := CurrentSolution.Machines[machineAIndex]
		machineB := CurrentSolution.Machines[machineBIndex]
		for _, instA := range machineA.AppKV {
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
			//如果machineA上的appA已经迁移到machineB了，则重新生成
			if duplicate {
				continue
			}
			canMove, totalScore := s.tryMove(instA, machineB, CurrentSolution, false)
			uinstA := s.UnchangedSolution.InstKV[instA.Id]
			umachineB := s.UnchangedSolution.Machines[machineBIndex]
			ucanMove, _ := s.tryMove(uinstA, umachineB, s.UnchangedSolution, false)
			if canMove && ucanMove{ 
				s.getNewNeibor(i, instA, machineA, machineB, totalScore)
				moved = true
				break //产生一个邻居后，继续产生下一个邻居
			}
		}
		if !moved {
			failIter++
			i--
			s.Neibors = s.Neibors[:len(s.Neibors)-1]
		}
		if failIter > 5000 {
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
			machineBIndex = rand.Intn(4000) + 2000
		}
	} else if s.Dataset == "e" {
		rate := rand.Intn(100)
		if rate > 50 {
			machineBIndex = rand.Intn(2000) + 6000
		} else {
			machineBIndex = rand.Intn(4000) + 2000
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
	//移动过后，m1少了一个inst，而m2多了一个inst。算分的时候要注意
	score1 := CpuScore(cpu1[0:98], m1.CpuCapacity, len(m1.InstKV)-1)
	score2 := CpuScore(cpu2[0:98], m2.CpuCapacity, len(m2.InstKV)+1)
	delta := score1 + score2 - (m1.Score() + m2.Score())

	if !force && (delta > 0 || -delta < 0.0001) {
		return false, 1e9
	}

	return true, solution.TotalScore + delta
}


func (s *Scheduler) Output(Dataset string) {

	filePath := fmt.Sprintf("submit_file_%s.csv", Dataset)
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
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
	logrus.Infof("writing to %s\n", filePath)
}


