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
	UnchangedSol *Solution //在移动inst的过程中，不将inst从其原来的machine上删除. 只用来判断是否有冲突以及是否有资源超额
	BestSol      *Solution
	CurrentSol   *Solution //记录当前解，主要用来判断分数是否有下降，跟UnchangedSol配合使用
	BestScore    float64

	TabuKv        map[ExchangeApp]int //禁忌表
	SubmitResult  []SubmitResult
	PendingInstKv map[string]string  //记录inst原来部署的机器的machineId

	DataSet    string
	SubmitFile string
	Round int
	SubmitRound int
	EveryRoundSize []int

}

func NewScheduler(round int, dataSet, submitFile string, machines []*Machine, instKV map[string]*Instance, appKV map[string]*Application, machineKV map[string]*Machine) *Scheduler {
	solution := &Solution{
		InstKV:    instKV,
		AppKV:     appKV,
		MachineKV: machineKV,
		Machines:  machines,
	}

	sch := &Scheduler{
		InitSol:       solution,
		DataSet:       dataSet,
		SubmitFile:    submitFile,
		Round:         round,
		TabuKv:        make(map[ExchangeApp]int),
		PendingInstKv: make(map[string]string),
	}
	sch.InitSol.TotalScore = TotalScore(sch.InitSol.Machines) //原始解的分数
	sch.InitSol.PermitValue = sch.InitSol.TotalScore
	sch.UnchangedSol = CopySolution(sch.InitSol)

	sch.SubmitRound = sch.readSubmitFile(submitFile)  //优化：从submitFile中读取inst迁移过程，节省迁移搜索时间

	return sch
}

//SubmitFile: submit file;
func (s *Scheduler) readSubmitFile(SubmitFile string) int{
	if _, err := os.Open(SubmitFile); err!= nil {
		return -1
	}
	lines := ReadLines(SubmitFile)
	round := 1
	sizeCount := 0
	for _, line := range lines {
		//if line == "\n" { logrus.Infof("empty line")}
		split := strings.Split(line, ",")
		if inRound, _ := strconv.Atoi(split[0]); round == inRound {
			sizeCount++
			s.moveInstViaSubmitFile(split[1], split[2], inRound)
		} else {
			s.EveryRoundSize = append(s.EveryRoundSize, sizeCount)
			round++
			s.prepareNextRound()
			s.moveInstViaSubmitFile(split[1], split[2], inRound)
		}
	}
	s.EveryRoundSize = append(s.EveryRoundSize, sizeCount)

	s.InitSol.TotalScore = TotalScore(s.InitSol.Machines)
	s.InitSol.PermitValue = s.InitSol.TotalScore
	return round
}

func (s *Scheduler) prepareNextRound() {
	for instId := range s.PendingInstKv {
		inst := s.InitSol.InstKV[instId]
		inst.Exchanged = false
	}

	s.UnchangedSol = nil
	s.UnchangedSol = CopySolution(s.InitSol)
	s.PendingInstKv = nil
	s.PendingInstKv = make(map[string]string)
}

func (s *Scheduler) moveInstViaSubmitFile(instId, machineId string, round int) {
	inst := s.InitSol.InstKV[instId]
	machine := s.InitSol.MachineKV[machineId]

	s.PendingInstKv[inst.Id] = inst.Machine.Id
	submitA := SubmitResult{round, inst.Id, machine.Id, inst.Machine.Id}
	s.SubmitResult = append(s.SubmitResult, submitA)

	machine.Put(inst, true)

	uinst := s.UnchangedSol.InstKV[instId]
	umachine := s.UnchangedSol.MachineKV[machineId]
	//if instId == "inst_84480" {
	//	logrus.Infof("%f + %f > %f", umachine.Usage[98], uinst.App.Mem[0], umachine.Capacity[98])
	//	logrus.Infof("%f + %f > %f", machine.Usage[98], inst.App.Mem[0], machine.Capacity[98])
	//	ucanMove, _ := s.tryMove(uinst, umachine, s.UnchangedSol, true)
	//	logrus.Infof("%s can move to %s: %t", instId, machineId, ucanMove)
	//}
	umachine.Put(uinst, false)

}

func (s *Scheduler) TabuSearch() {
	s.StartSearch()
}

func (s *Scheduler) StartSearch() {
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

	var localBestCandidate *Candidate
	var localBestSolution *Solution

	logrus.Infof("CurrentSol score: %f", TotalScore(s.BestSol.Machines))
	logrus.Infof("round: %d", round)
	for {
		iter++
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
		machineA := instA.Machine
		machineB := localBestSolution.MachineKV[localBestCandidate.MachineB]
		umachineB := s.UnchangedSol.MachineKV[localBestCandidate.MachineB]
		//如果inst已经被移动了，则不再移动该inst
		if instA.Exchanged {
			continue
		}
		//todo: uinstA迁移之后居然对instA的迁移产生了影响：已经确定是复制solution之后，solution之间是有干扰的
		//todo: 是复制solution的时候即CopySolution()方法里面machine.appCntKV没有重新创建
		s.PendingInstKv[uinstA.Id] = uinstA.Machine.Id
		umachineB.Put(uinstA, false)
		machineB.Put(instA, true) //todo: put的时候，偶尔会出现内存超出的情况

		localBestSolution.TotalScore = localBestCandidate.TotalScore
		s.UnchangedSol.TotalScore = localBestCandidate.TotalScore

		submitA := SubmitResult{round, instA.Id, machineB.Id, machineA.Id}
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
			s.BestScore = localBestCandidate.TotalScore
			s.BestSol = nil
			s.BestSol = CopySolution(localBestSolution)
		}

		s.CurrentSol = localBestSolution
		logrus.Infof("Local best solution score: %.8f\n", localBestCandidate.TotalScore)

		s.backSpace(iter)
	}

}

func (s *Scheduler) backSpace(iter int)  {
	if iter % 50 == 0 {
		startIndex := 0
		//logrus.Infof("EveryRoundSize: %d", len(s.EveryRoundSize))
		if s.Round != 1 {
			startIndex = s.EveryRoundSize[s.Round-2]  //即上一轮迁移的inst的数目，因为是上一轮迁移的，故本轮中startIndex是不会变的
		}
		roundSize := len(s.SubmitResult[startIndex:])
		index := rand.Intn(roundSize) + startIndex
		//logrus.Infof("startIndex: %d", index)
		submit := s.SubmitResult[index]
		//logrus.Infof("submit: %s", submit)
		inst := s.CurrentSol.InstKV[submit.Instance]
		machineFrom := s.CurrentSol.MachineKV[submit.MachineFrom]

		uinst := s.UnchangedSol.InstKV[submit.Instance]
		umachineB := s.UnchangedSol.MachineKV[submit.Machine]

		// 不用判断能不能撤销这一步，因为在s.UnchangedSol中inst原来的机器上依然保留着inst，故inst原来的机器上是可以容纳inst的，所以直接迁移回去即可
		machineFrom.Put(inst, true)
		inst.Exchanged = false
		s.CurrentSol.TotalScore = TotalScore(s.CurrentSol.Machines)
		s.CurrentSol.PermitValue = s.CurrentSol.TotalScore
		umachineB.Remove(uinst) //因为uinst没有从原来的机器上删除，故只需要把umachineB上的uinst删除即可

		s.SubmitResult = append(s.SubmitResult[:index], s.SubmitResult[index+1:]...)
		//s.CurrentSol = CopySolution(s.BestSol)
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
	s.Neighbors = s.Neighbors[:0:0] //优化：生成neighbors之前先清空，避免neighbors臃肿
	failIter := 0
	for i := 0; i < InitNeighborSize; i++ {
		machineAIndex := rand.Intn(len(s.BestSol.Machines))
		machineBIndex := s.getMachineBIndex() //优化：针对不同的数据集使用不同的策略产生目标机器：目标机器为大机器的概率更大
		if machineAIndex == machineBIndex {
			i--
			continue
		}
		newNeighbor := new(Candidate) //优化：只记录邻居相对currentSolution的单步移动，避免记录整个solution导致内存开销过大
		newNeighbor.TotalScore = 1e9
		s.Neighbors = append(s.Neighbors, newNeighbor)
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
			for _, Neighbor := range s.Neighbors {
				if Neighbor.MoveApp == MoveAppFromMachineAToB {
					logrus.Infof("%s had been moved", MoveAppFromMachineAToB)
					duplicate = true
					break
				}
			}
			//如果machineA上的appA已经迁移到machineB了，则重新生成
			if duplicate {
				continue
			}
			canMove, totalScore := s.tryMove(instA, machineB, CurrentSolution, false) //主要用来判断是否能移动
			uinstA := s.UnchangedSol.InstKV[instA.Id]
			umachineB := s.UnchangedSol.Machines[machineBIndex]
			ucanMove, _ := s.tryMove(uinstA, umachineB, s.UnchangedSol, true)  //UnchangedSol只用来判断是否有冲突以及是否有资源超额
			//logrus.Infof("%t %t", canMove, ucanMove)
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
		//if failIter > 5000 {
		//	return false
		//}
	}
	return true
}

func (s *Scheduler) getMachineBIndex() int {
	machineBIndex := 0
	if s.DataSet == "c" || s.DataSet == "d" {
		rate := rand.Intn(100)
		if rate > 45 {
			machineBIndex = rand.Intn(3000) + 6000
		} else {
			machineBIndex = rand.Intn(6000)
		}
	} else if s.DataSet == "e" {
		rate := rand.Intn(100)
		if rate > 60 {
			machineBIndex = rand.Intn(2000) + 6000
		} else {
			machineBIndex = rand.Intn(6000)
		}
	} else {
		//a, b的分数都在5000以下，故目标机器的范围不需要太大
		machineBIndex = rand.Intn(8000)
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
	//移动过后，m1少了一个inst，而m2多了一个inst。算分的时候要注意
	score1 := CpuScore(cpu1[0:98], m1.CpuCapacity, len(m1.InstKV)-1)
	score2 := CpuScore(cpu2[0:98], m2.CpuCapacity, len(m2.InstKV)+1)
	delta := score1 + score2 - (m1.GetScore() + m2.GetScore())
	//logrus.Infof("delta: %f %f %f", delta, m1.GetScore(), m2.GetScore())

	if !force && (delta > 0 || -delta < 0.0001) {
		//logrus.Infof("tryMove")
		return false, 1e9
	}

	return true, solution.TotalScore + delta
}

func (s *Scheduler) Output(dataSet string) {

	filePath := fmt.Sprintf("submit_%s_%.0f.csv", dataSet, s.BestScore)
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0600)
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
