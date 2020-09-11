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
	InitSol      *Solution
	UnchangedSol *Solution //在移动inst的过程中，不将inst从其原来的machine上删除. 只用来判断是否有冲突以及是否有资源超额
	BestSol      *Solution
	CurrentSol   *Solution //记录当前解，主要用来判断分数是否有下降，跟UnchangedSol配合使用
	BestScore    float64

	TabuKv        *TabuList //禁忌表
	SubmitResult  []SubmitResult
	PendingInstKv map[string]string //记录inst原来部署的机器的machineId

	DataSet        string
	SubmitFile     string
	Round          int
	SubmitRound    int
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
		PendingInstKv: make(map[string]string),
		TabuKv:        new(TabuList),
	}
	sch.TabuKv.Tabu = make(map[ExchangeApp]int)
	sch.InitSol.TotalScore = TotalScore(sch.InitSol.Machines) //原始解的分数
	sch.InitSol.PermitValue = sch.InitSol.TotalScore
	sch.UnchangedSol = CopySolution(sch.InitSol)

	sch.readSubmitFile(submitFile) //优化：从submitFile中读取inst迁移过程，节省迁移搜索时间

	sch.BestSol = CopySolution(sch.InitSol)

	return sch
}

//SubmitFile: submit file;
func (s *Scheduler) readSubmitFile(SubmitFile string) {
	if _, err := os.Open(SubmitFile); err != nil {
		return
	}
	lines := ReadLines(SubmitFile)
	round := 1
	sizeCount := 0
	for _, line := range lines {
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
	s.InitSol.SubmitResult = s.SubmitResult
	s.SubmitRound = round

	if s.SubmitRound < s.Round {
		s.prepareNextRound()
		logrus.Infof("Start next round ...")
	} else if s.SubmitRound > s.Round {
		logrus.Infof("invalid round! ")
		os.Exit(0)
	}
}

func (s *Scheduler) prepareNextRound() {
	for _, inst := range s.InitSol.InstKV {
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
	umachine.Put(uinst, false)

}

func (s *Scheduler) TabuSearch() {
	s.StartSearch()
}

func (s *Scheduler) StartSearch() {
	CurrentSol := CopySolution(s.BestSol)
	CurrentUnchangedSol := CopySolution(s.UnchangedSol)

	round := s.Round
	iter := 0

	var localBestCandidate *Candidate
	var localBestSolution *Solution

	logrus.Infof("CurrentSol score: %f", TotalScore(s.InitSol.Machines))
	logrus.Infof("round: %d", round)
	for {
		iter++
		//得到候选集
		if !s.getInitNeighbor(CurrentSol, CurrentUnchangedSol) {
			continue
		}
		s.getCandidateNeighbor(CurrentSol)
		//初始化候选集
		s.initCandidateSet(CurrentSol)
		//从候选集中选择分数最低的解
		localBestCandidate = s.getMinScoreSolution(CurrentSol)
		//移动inst
		localBestSolution = CurrentSol
		instA := localBestSolution.InstKV[localBestCandidate.InstA]
		uinstA := CurrentUnchangedSol.InstKV[localBestCandidate.InstA]
		machineA := instA.Machine
		machineB := localBestSolution.MachineKV[localBestCandidate.MachineB]
		umachineB := CurrentUnchangedSol.MachineKV[localBestCandidate.MachineB]

		//todo: uinstA迁移之后居然对instA的迁移产生了影响：已经确定是复制solution之后，solution之间是有干扰的
		//todo: 是复制solution的时候即CopySolution()方法里面machine.appCntKV没有重新创建
		s.PendingInstKv[uinstA.Id] = uinstA.Machine.Id
		umachineB.Put(uinstA, false)
		machineB.Put(instA, true) //todo: put的时候，偶尔会出现内存超出的情况

		localBestSolution.TotalScore = localBestCandidate.TotalScore
		CurrentUnchangedSol.TotalScore = localBestCandidate.TotalScore

		submitA := SubmitResult{round, instA.Id, machineB.Id, machineA.Id}
		localBestSolution.SubmitResult = append(localBestSolution.SubmitResult, submitA)

		//设置局部最优解的特赦值
		if localBestCandidate.TotalScore < CurrentSol.PermitValue {
			localBestSolution.PermitValue = localBestCandidate.TotalScore
		} else if CurrentSol.TotalScore < localBestCandidate.PermitValue {
			localBestSolution.PermitValue = CurrentSol.TotalScore
		}
		//更新tabulist
		s.updateTabuList()
		s.TabuKv.Lock.Lock()
		s.TabuKv.Tabu[localBestCandidate.MoveApp] = TabuLen
		s.TabuKv.Lock.Unlock()

		//如果局部最优解优于当前最优解，则将局部最优解设置为当前最优解
		if localBestCandidate.TotalScore < s.BestSol.TotalScore-1e-9 {
			logrus.Infof("New best solution: %0.8f --> %0.8f\n", s.BestSol.TotalScore, localBestCandidate.TotalScore)

			//更新最优解
			s.BestSol = localBestSolution
			s.BestScore = localBestCandidate.TotalScore
			s.UnchangedSol = CurrentUnchangedSol

			//更新全局SubmitResult
			s.SubmitResult = make([]SubmitResult, len(s.BestSol.SubmitResult))
			copy(s.SubmitResult, s.BestSol.SubmitResult)

			CurrentSol = nil
			CurrentSol = CopySolution(s.BestSol)
		}

		logrus.Infof("Local best solution score: %.8f\n", localBestCandidate.TotalScore)
		//优化：与BestSol同步，避免当前解陷入局部最优跳不出来
		if iter%SyncIter == 0 {
			CurrentSol = nil
			CurrentSol = CopySolution(s.BestSol)
			CurrentUnchangedSol = nil
			CurrentUnchangedSol = CopySolution(s.UnchangedSol)
		}
		s.backSpace(iter, CurrentSol, CurrentUnchangedSol)
	}

}

//todo: 跨轮次undo操作
func (s *Scheduler) backSpace(iter int, CurrentSol *Solution, CurrentUnchangedSol *Solution) {
	if iter%BackSpaceLen == 0 { //不宜太小，不然扰动太大
		startIndex := 0
		if s.Round != 1 {
			startIndex = s.EveryRoundSize[s.Round-2] //即上一轮迁移的inst的数目，因为是上一轮迁移的，故本轮中startIndex是不会变的
		}
		roundSize := len(CurrentSol.SubmitResult[startIndex:])
		index := 0
		if roundSize <= 0 {
			index = startIndex
		} else {
			index = rand.Intn(roundSize) + startIndex
		}
		submit := CurrentSol.SubmitResult[index]
		inst := CurrentSol.InstKV[submit.Instance]
		machineFrom := CurrentSol.MachineKV[submit.MachineFrom]

		uinst := CurrentUnchangedSol.InstKV[submit.Instance]
		umachineB := CurrentUnchangedSol.MachineKV[submit.Machine]

		// 不用判断能不能撤销这一步，因为在CurrentUnchangedSol中inst原来的机器上依然保留着inst，故inst原来的机器上是可以容纳inst的，所以直接迁移回去即可
		machineFrom.Put(inst, true)
		inst.Exchanged = false
		CurrentSol.TotalScore = TotalScore(CurrentSol.Machines)
		CurrentSol.PermitValue = CurrentSol.TotalScore
		umachineB.Remove(uinst) //因为uinst没有从原来的机器上删除，故只需要把umachineB上的uinst删除即可

		CurrentSol.SubmitResult = append(CurrentSol.SubmitResult[:index], CurrentSol.SubmitResult[index+1:]...)
		//CurrentSol = CopySolution(s.BestSol)
	}
}

func (s *Scheduler) initCandidateSet(CurrentSol *Solution) {
	s.TabuKv.Lock.Lock()
	defer s.TabuKv.Lock.Unlock()
	//初始化候选集
	if len(s.TabuKv.Tabu) == 0 {
		for _, candidateX := range CurrentSol.Candidates {
			candidateX.IsCandidate = true
		}
	} else {
		for tabu := range s.TabuKv.Tabu {
			for _, candidateX := range CurrentSol.Candidates {
				if tabu == candidateX.MoveApp {
					logrus.Infof("%s was tabued", tabu)
					candidateX.IsCandidate = false
				} else {
					candidateX.IsCandidate = true
				}
				if candidateX.TotalScore < CurrentSol.PermitValue { //特赦规则
					candidateX.IsCandidate = true
				}
			}
		}
	}
}

func (s *Scheduler) getMinScoreSolution(CurrentSol *Solution) *Candidate {
	var localBestCandidate *Candidate
	i := 0
	for _, candidateS := range CurrentSol.Candidates {
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
	s.TabuKv.Lock.Lock()
	defer s.TabuKv.Lock.Unlock()
	for key := range s.TabuKv.Tabu {
		s.TabuKv.Tabu[key]--
		if s.TabuKv.Tabu[key] == 0 {
			delete(s.TabuKv.Tabu, key)
		}
	}
}

func (s *Scheduler) getInitNeighbor(CurrentSolution *Solution, CurrentUnchangedSol *Solution) bool {
	CurrentSolution.Neighbors = CurrentSolution.Neighbors[:0:0] //优化：生成neighbors之前先清空，避免neighbors臃肿
	failIter := 0
	for i := 0; i < InitNeighborSize; i++ {
		machineAIndex := rand.Intn(len(s.InitSol.Machines))
		machineBIndex := s.getMachineBIndex() //优化：针对不同的数据集使用不同的策略产生目标机器：目标机器为大机器的概率更大
		if machineAIndex == machineBIndex {
			i--
			continue
		}
		newNeighbor := new(Candidate) //优化：只记录邻居相对currentSolution的单步移动，避免记录整个solution导致内存开销过大
		newNeighbor.TotalScore = 1e9
		CurrentSolution.Neighbors = append(CurrentSolution.Neighbors, newNeighbor)
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
			for _, Neighbor := range CurrentSolution.Neighbors {
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

			//注意这里canMove和ucanMove，ucanMove的force参数必须是true
			canMove, totalScore := s.tryMove(instA, machineB, CurrentSolution, AllowScoreDecrease) //主要用来判断是否能移动
			uinstA := CurrentUnchangedSol.InstKV[instA.Id]
			umachineB := CurrentUnchangedSol.Machines[machineBIndex]
			ucanMove, _ := s.tryMove(uinstA, umachineB, CurrentUnchangedSol, true) //UnchangedSol只用来判断是否有冲突以及是否有资源超额
			//logrus.Infof("%t %t", canMove, ucanMove)
			if canMove && ucanMove {
				s.getNewNeighbor(i, instA, machineA, machineB, totalScore, CurrentSolution)
				moved = true
				break //产生一个邻居后，继续产生下一个邻居
			}
		}
		if !moved {
			failIter++
			i--
			CurrentSolution.Neighbors = CurrentSolution.Neighbors[:len(CurrentSolution.Neighbors)-1]
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
		if rate > 40 {
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
		machineBIndex = rand.Intn(8000)
	}
	return machineBIndex
}

func (s *Scheduler) getLargeMachineIndex() int {
	if s.DataSet == "c" || s.DataSet == "d" {
		return 6000
	} else if s.DataSet == "e" {
		return 6000
	} else {
		return 0
	}
}

func (s *Scheduler) getNewNeighbor(index int, instA *Instance, machineA, machineB *Machine, totalScore float64, CurrentSol *Solution) {
	CurrentSol.Neighbors[index].InstA = instA.Id
	CurrentSol.Neighbors[index].MachineA = machineA.Id
	CurrentSol.Neighbors[index].MachineB = machineB.Id
	CurrentSol.Neighbors[index].TotalScore = totalScore
	CurrentSol.Neighbors[index].IsCandidate = false
	CurrentSol.Neighbors[index].PermitValue = totalScore
	CurrentSol.Neighbors[index].MoveApp = ExchangeApp{
		instA.AppId,
		machineA.Id,
		machineB.Id,
	}
}

func (s *Scheduler) getCandidateNeighbor(CurrentSol *Solution) {
	Neighbors := NeighborSlice(CurrentSol.Neighbors[:])
	sort.Sort(Neighbors)
	CurrentSol.Candidates = Neighbors[:CandidateLen]
	for _, candidate := range CurrentSol.Candidates {
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

	if !force && (delta > 0 || -delta < 0.00001) {
		return false, 1e9
	}

	return true, solution.TotalScore + delta
}

func (s *Scheduler) HandleHighCpuMachines() {
	machineLen := len(s.BestSol.Machines)
	largeMachineIndex := s.getLargeMachineIndex()
	logrus.Infof("largeMachineIndex: ", largeMachineIndex)
	for _, machine := range s.BestSol.Machines {
		if len(machine.InstKV) == 0 {
			continue
		}
		averageCpuUtil := s.calculateMachineCpuUtil(machine)
		if averageCpuUtil > 0.6 {
			logrus.Infof("high cpu util: %s--%f", machine.Id, averageCpuUtil)
			for _, inst := range machine.InstKV {
				averageCpuUtil := s.calculateMachineCpuUtil(machine)
				if averageCpuUtil < 0.6 {
					logrus.Infof("high cpu util: %s--%f", machine.Id, averageCpuUtil)
					break
				}
				//先放大机器，后放小机器
				moved := false
				for j := largeMachineIndex; j < machineLen; j++ {
					if s.moveToDestMachine(j, inst, machine, true) {
						moved = true
						break
					}
				}
				if !moved {
					for j := 0; j < largeMachineIndex; j++ {
						if s.moveToDestMachine(j, inst, machine, true) {
							moved = true
							break
						}

					}
				}
			}
		}

	}
}

func (s *Scheduler) moveToDestMachine(machineIndex int, inst *Instance, machine *Machine, allowScoreDecrease bool) bool {
	machineB := s.BestSol.Machines[machineIndex]
	cpu := s.calculateMachineCpuUtil(machineB)
	if cpu > 0.5 {
		return false
	}

	//注意这里canMove和ucanMove，ucanMove的force参数必须是true
	canMove, _ := s.tryMove(inst, machineB, s.BestSol, allowScoreDecrease) //主要用来判断是否能移动
	uinstA := s.UnchangedSol.InstKV[inst.Id]
	umachineB := s.UnchangedSol.Machines[machineIndex]
	ucanMove, _ := s.tryMove(uinstA, umachineB, s.BestSol, true) //UnchangedSol只用来判断是否有冲突以及是否有资源超额

	if canMove && ucanMove {
		submitA := SubmitResult{s.Round, inst.Id, machineB.Id, machine.Id}
		s.SubmitResult = append(s.SubmitResult, submitA)
		s.BestSol.SubmitResult = append(s.BestSol.SubmitResult, submitA)

		machineB.Put(inst, true)
		umachineB.Put(uinstA, false)

		//oldScore := s.BestSol.TotalScore
		s.BestSol.TotalScore = TotalScore(s.BestSol.Machines)
		s.BestSol.PermitValue = s.BestSol.TotalScore
		s.BestScore = s.BestSol.TotalScore
		return true
	}
	return false
}

func (s *Scheduler) calculateMachineCpuUtil(machine *Machine) float64 {
	var averageCpuUtil = 0.0
	for _, cpuUtil := range machine.CpuUsage() {
		averageCpuUtil += cpuUtil
	}
	averageCpuUtil /= 98
	averageCpuUtil /= machine.CpuCapacity
	return averageCpuUtil
}

// Output
func (s *Scheduler) Output(dataSet string) {

	filePath := fmt.Sprintf("submit_%s_round%d_%.0f.csv", dataSet, s.Round, s.BestScore)
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	if s.BestSol == nil {
		for _, submit := range s.SubmitResult {
			line := fmt.Sprintf("%d,%s,%s", submit.Round, submit.Instance, submit.Machine)
			fmt.Fprintln(w, line)
		}
	} else {
		for _, submit := range s.BestSol.SubmitResult {
			line := fmt.Sprintf("%d,%s,%s", submit.Round, submit.Instance, submit.Machine)
			fmt.Fprintln(w, line)
		}
	}

	w.Flush()
	logrus.Infof("writing to %s\n", filePath)
}
