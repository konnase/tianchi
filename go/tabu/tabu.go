package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	MachineInput         = "data/scheduling_semifinal_data_20180815/machine_resources.a.csv"
	InstanceInput        = "data/scheduling_semifinal_data_20180815/instance_deploy.a.csv"
	ApplicationInput     = "data/scheduling_semifinal_data_20180815/app_resources.csv"
	AppInterferenceInput = "data/scheduling_semifinal_data_20180815/app_interference.csv"
	JobInfoInput         = "data/scheduling_semifinal_data_20180815/job_info.a.csv"
	InitNeiborSet        = "data/init_neibor_set.csv"
	InitNeiborSize       = 100
	CandidateLen         = 80
	TabuLen              = 4
)

type Instance struct {
	Id      string
	App     *Application
	AppId   string
	Machine *Machine
	MachineId string

	deployed bool
	exchanged int
	//lock     sync.Mutex
}

func NewInstance(line string) *Instance {
	line = strings.TrimSpace(line)
	splits := strings.Split(line, ",")
	return &Instance{
		Id:    splits[0],
		AppId: splits[1],
		MachineId: splits[2],
	}
}

type Application struct {
	Id        string
	Cpu       [98]float64
	Mem       [98]float64
	Disk      float64
	Instances []Instance
	Resource  [200]float64
}

func NewApplication(line string) *Application {
	line = strings.TrimSpace(line)
	splits := strings.Split(line, ",")
	app := &Application{
		Id: splits[0],
	}
	points := strings.Split(splits[1], "|")
	for i, p := range points {
		app.Cpu[i], _ = strconv.ParseFloat(p, 64)
	}
	points = strings.Split(splits[2], "|")
	for i, p := range points {
		app.Mem[i], _ = strconv.ParseFloat(p, 64)
	}
	app.Disk, _ = strconv.ParseFloat(splits[3], 64)
	for i := 0; i < 98; i++ {
		app.Resource[i] = app.Cpu[i]
	}
	for i := 99; i < 196; i++ {
		app.Resource[i] = app.Mem[i-98]
	}
	app.Resource[196] = app.Disk
	app.Resource[197], _ = strconv.ParseFloat(splits[4], 64)
	app.Resource[198], _ = strconv.ParseFloat(splits[5], 64)
	app.Resource[199], _ = strconv.ParseFloat(splits[6], 64)
	return app
}

type AppInterference map[string]map[string]int

type Machine struct {
	Id           string
	CpuCapacity  float64
	MemCapacity  float64
	DiskCapacity float64
	PmpCapacity  [3]float64
	Capacity     [200]float64
	Usage        [200]float64

	instKV          map[string]*Instance
	appCntKV        map[string]int
	appKV           map[string]*Instance //<appId, onlyOneInstance>
	appInterference AppInterference

	lock sync.Mutex
}

type MachineSlice []*Machine

func (m MachineSlice) Len() int {
	return len(m)
}

func (m MachineSlice) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}
func (m MachineSlice) Less(i, j int) bool {
	return m[i].DiskCapacity > m[j].DiskCapacity
}

func NewMachine(line string, interference AppInterference) *Machine {
	line = strings.TrimSpace(line)
	splits := strings.Split(line, ",")
	machine := &Machine{
		Id:              splits[0],
		instKV:          make(map[string]*Instance),
		appCntKV:        make(map[string]int),
		appKV:           make(map[string]*Instance),
		appInterference: interference,
	}
	machine.CpuCapacity, _ = strconv.ParseFloat(splits[1], 64)
	machine.MemCapacity, _ = strconv.ParseFloat(splits[2], 64)
	machine.DiskCapacity, _ = strconv.ParseFloat(splits[3], 64)
	machine.PmpCapacity[0], _ = strconv.ParseFloat(splits[4], 64)
	machine.PmpCapacity[1], _ = strconv.ParseFloat(splits[5], 64)
	machine.PmpCapacity[2], _ = strconv.ParseFloat(splits[6], 64)
	for i := 0; i < 98; i++ {
		machine.Capacity[i] = machine.CpuCapacity
	}
	for i := 98; i < 196; i++ {
		machine.Capacity[i] = machine.MemCapacity
	}
	machine.Capacity[196] = machine.DiskCapacity
	for i := 197; i < 200; i++ {
		machine.Capacity[i] = machine.PmpCapacity[i-197]
	}
	return machine
}

func (m *Machine) cpuUsage() []float64 {
	return m.Usage[0:98]
}

func (m *Machine) memUsage() []float64 {
	return m.Usage[99:196]
}

func (m *Machine) diskUsage() float64 {
	return m.Usage[196]
}

func (m *Machine) pmpUsage() []float64 {
	return m.Usage[197:200]
}

func (m *Machine) isCpuOverload() bool {
	for _, v := range m.cpuUsage() {
		if v > m.CpuCapacity {
			return true
		}
	}
	return false
}

func (m *Machine) isMemOverload() bool {
	for _, v := range m.memUsage() {
		if v > m.MemCapacity {
			return true
		}
	}
	return false
}

func (m *Machine) isDiskOverload() bool {
	return m.diskUsage() > m.DiskCapacity
}

func (m *Machine) isPmpOverload() bool {
	for i, v := range m.pmpUsage() {
		if v > m.PmpCapacity[i] {
			return true
		}
	}
	return false
}

func (m *Machine) isOverload() bool {
	return m.isCpuOverload() || m.isMemOverload() || m.isDiskOverload() || m.isPmpOverload()
}

func (m *Machine) score() float64 {
	if m.diskUsage() == 0 {
		return 0.0
	}
	return cpuScore(m.cpuUsage(), m.CpuCapacity, len(m.instKV))
}

func cpuScore(cpuUsage []float64, cpuCapacity float64, instNum int) float64 {
	score := 0.0
	for _, v := range cpuUsage {
		util := v / cpuCapacity
		alpha := float64(instNum + 1)
		score += 1 + alpha*(math.Exp(math.Max(util-0.5, 0))-1)

	}
	return score / 98.0
}

func (m *Machine) put(inst *Instance) {
	if _, ok := m.instKV[inst.Id]; ok {
		return
	}

	for i := 0; i < 200; i++ {
		m.Usage[i] += inst.App.Resource[i]
	}

	if inst.Machine != nil {
		inst.Machine.remove(inst)
	}

	inst.Machine = m
	inst.deployed = true
	inst.exchanged += 1

	m.instKV[inst.Id] = inst
	m.appKV[inst.App.Id] = inst //每类应用只记录一个实例用来swap即可

	if _, ok := m.appCntKV[inst.App.Id]; ok {
		m.appCntKV[inst.App.Id] += 1
	} else {
		m.appCntKV[inst.App.Id] = 1
	}
}

func (m *Machine) remove(inst *Instance) {
	if _, ok := m.instKV[inst.Id]; !ok {
		return
	}

	for i := 0; i < 200; i++ {
		m.Usage[i] -= inst.App.Resource[i]
	}

	inst.Machine = nil
	inst.deployed = false

	delete(m.instKV, inst.Id)

	m.appCntKV[inst.App.Id] -= 1
	if m.appCntKV[inst.App.Id] == 0 {
		delete(m.appCntKV, inst.App.Id)
		delete(m.appKV, inst.App.Id)
	}
}

func (m *Machine) canPutInst(inst *Instance) bool {
	return !m.outOfCapacityInst(inst) && !m.hasConflictInst(inst)
}

func (m *Machine) outOfCapacityInst(inst *Instance) bool {
	for i := 0; i < 200; i++ {
		if inst.App.Resource[i]+m.Usage[i] > m.Capacity[i] {
			return true
		}
	}
	return false
}

func (m *Machine) hasConflictInst(inst *Instance) bool {
	appNow := inst.App.Id
	appNowCnt := 0
	if _, ok := m.appCntKV[appNow]; ok {
		appNowCnt = m.appCntKV[appNow]
	}

	for appId, appCnt := range m.appCntKV {
		if appNowCnt+1 > m.conflictLimitOf(appId, appNow) {
			return true
		}
		if appCnt > m.conflictLimitOf(appNow, appId) {
			return true
		}
	}
	return false
}

func (m *Machine) conflictLimitOf(appIdA, appIdB string) int {
	if _, ok := m.appInterference[appIdA][appIdB]; ok {
		return m.appInterference[appIdA][appIdB]
	}
	return 1e9
}

func (m *Machine) hasConflict() bool {
	cnt := 0
	for appIdA, appCntA := range m.appCntKV {
		for appIdB, appCntB := range m.appCntKV {
			if appCntB > m.conflictLimitOf(appIdA, appIdB) {
				cnt += 1
			}
			if appCntA > m.conflictLimitOf(appIdB, appIdA) {
				cnt += 1
			}
		}
	}
	return cnt > 0
}

func (m *Machine) instDiskList() string {
	var disks []string
	for _, inst := range m.instKV {
		disks = append(disks, strconv.FormatInt(int64(inst.App.Disk), 10))
	}
	return strings.Join(disks, ",")
}

func (m *Machine) instIdList() string {
	var ids []string
	for _, inst := range m.instKV {
		ids = append(ids, inst.Id)
	}
	return strings.Join(ids, ",")
}

func TotalScore(machines []*Machine) float64 {
	s := 0.0
	for _, m := range machines {
		s += m.score()
	}
	return s
}

func ReadLines(input string) []string {
	f, err := os.Open(input)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	var lines []string
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}
		line = strings.TrimSpace(line)
		lines = append(lines, line)
	}
	return lines
}

func ReadAppInterference(lines []string) AppInterference {
	m := make(map[string]map[string]int)
	for _, line := range lines {
		splits := strings.Split(line, ",")
		if _, ok := m[splits[0]]; !ok {
			m[splits[0]] = make(map[string]int)
		}
		num, _ := strconv.Atoi(splits[2])
		if splits[0] == splits[1] {
			num++
		}
		m[splits[0]][splits[1]] = num
	}
	return m
}

func ReadMachine(lines []string, interference AppInterference) []*Machine {
	// m := make([]*Machine, 6000)
	var m []*Machine
	for _, line := range lines {
		m = append(m, NewMachine(line, interference))
	}
	return m
}

func ReadApplication(lines []string) []*Application {
	// a := make([]*Application, 9000)
	var a []*Application
	for _, line := range lines {
		a = append(a, NewApplication(line))
	}
	return a
}

func ReadInstance(lines []string) []*Instance {
	// i := make([]*Instance, 69000)
	var i []*Instance
	for _, line := range lines {
		i = append(i, NewInstance(line))
	}
	return i
}

func ReadData() ([]*Machine, map[string]*Instance, map[string]*Application, map[string]*Machine) {
	interference := ReadAppInterference(ReadLines(AppInterferenceInput))
	machines := ReadMachine(ReadLines(MachineInput), interference)
	machineKV := make(map[string]*Machine)
	for _, machine := range machines {
		machineKV[machine.Id] = machine
	}

	apps := ReadApplication(ReadLines(ApplicationInput))
	appKV := make(map[string]*Application)
	for _, app := range apps {
		appKV[app.Id] = app
	}

	insts := ReadInstance(ReadLines(InstanceInput))
	instKV := make(map[string]*Instance)
	for _, inst := range insts {
		inst.App = appKV[inst.AppId]
		inst.Machine = machineKV[inst.MachineId]
		inst.Machine.put(inst)
		inst.exchanged = 0
		instKV[inst.Id] = inst
	}

	return machines, instKV, appKV, machineKV
}

type ExchangeApp struct {
	appA     string
	machineA string
	appB     string
	machineB string
}

type SubmitResult struct {
	round int
	instance string
	machine string
}

type Candidate struct {
	instA string
	instB string

	totalScore float64
	isCandidate bool
	permitValue float64

	exchangeAppFromA ExchangeApp //记录这个解的移动的方向
	exchangeAppFromB ExchangeApp //记录这个解的移动的方向
	exchangeAppToA   ExchangeApp //记录这个解的移动的方向
	exchangeAppToB   ExchangeApp //记录这个解的移动的方向
}
type Solution struct {
	machines    []*Machine
	instKV      map[string]*Instance
	appKV       map[string]*Application
	machineKV   map[string]*Machine
	totalScore  float64 //todo: 共享变量，不影响计算过程，只用于输出
	permitValue float64

	lock sync.RWMutex
}

type NeiborSlice []*Candidate

func (s NeiborSlice) Len() int {
	return len(s)
}

func (s NeiborSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s NeiborSlice) Less(i, j int) bool {
	return s[i].totalScore < s[j].totalScore
}

type Scheduler struct {
	solutions    []*Solution
	neibors []*Candidate
	candidates   []*Candidate
	bestSolution *Solution //固定位置solutions[0]
	nowSolution  *Solution //固定位置solutions[1]

	tabuList        map[ExchangeApp]int //禁忌表
	searchResult map[string]string
	submitResult []SubmitResult

	searchFile string
	lock       sync.RWMutex
}

func NewScheduler(searchFile string, machine []*Machine, instKV map[string]*Instance, appKV map[string]*Application, machineKV map[string]*Machine) *Scheduler {
	solution := &Solution{
		instKV:      instKV,
		appKV:       appKV,
		machineKV:   machineKV,
		machines:    machine,
	}
	scheduler := &Scheduler{
		searchFile: searchFile,
		tabuList:   make(map[ExchangeApp]int),
		searchResult: make(map[string]string),
	}
	scheduler.solutions = append(scheduler.solutions, solution)
	scheduler.solutions = append(scheduler.solutions, solution)
	scheduler.bestSolution = scheduler.solutions[0]
	scheduler.nowSolution = scheduler.solutions[1]
	scheduler.bestSolution.permitValue = scheduler.bestSolution.totalScore

	scheduler.readSearchFile(searchFile, 0)

	scheduler.solutions[0].totalScore = TotalScore(scheduler.solutions[0].machines) //原始解的分数
	return scheduler
}

func (s *Scheduler) tabuSearch() {
	// fmt.Println(s.candidateSetLen)
	// s.getInitNeiborSet(s.bestSolution)
	// s.getCandidateNeibor()
	// fmt.Println(len(s.candidates))
	// for _, candidate := range s.solutions {
	// 	// change := ExchangeApp{candidate.exchangeAppFromA.appA, candidate.exchangeAppFromA.machineA, candidate.exchangeAppFromB.appB, candidate.exchangeAppFromA.machineB}
	// 	fmt.Println(candidate.isCandidate)
	// 	// fmt.Println(candidate.exchangeAppFromA, candidate.exchangeAppToA)
	// 	// fmt.Println(candidate.exchangeAppFromA == change)
	// 	fmt.Println(TotalScore(candidate.machines))
	// 	// fmt.Println(&candidate)
	// }

	s.startSearch()
}

func (s *Scheduler) startSearch() {
	nowSolution := s.bestSolution
	var localBestCandidate *Candidate
	var localBestSolution *Solution
	round := 0
	for {
		//得到候选集
		s.getInitNeiborSet(nowSolution)
		s.getCandidateNeibor()
		//初始化候选集
		round++
		// fmt.Printf("Round %d\n", round)
		if len(s.tabuList) == 0 {
			for _, candidateX := range s.candidates {
				candidateX.isCandidate = true
			}
		} else {
			s.lock.Lock()
			for tabu := range s.tabuList {
				for _, candidateX := range s.candidates {
					if tabu == candidateX.exchangeAppFromA || tabu == candidateX.exchangeAppToA ||
						tabu == candidateX.exchangeAppFromB || tabu == candidateX.exchangeAppToB {
						// fmt.Printf("enter tabu:\n")
						// os.Exit(0)
						candidateX.isCandidate = false
					} else {
						candidateX.isCandidate = true
					}
					if candidateX.totalScore < nowSolution.permitValue { //特赦规则
						candidateX.isCandidate = true
					}
				}
			}
			s.lock.Unlock()
		}
		//从候选集中选择分数最低的解
		i := 0
		for _, candidateS := range s.candidates {
			// println(candidateS.totalScore)
			if candidateS.isCandidate {
				if i == 0 {
					localBestCandidate = candidateS
				}
				if candidateS.totalScore < localBestCandidate.totalScore {
					localBestCandidate = candidateS
				}
				i++
			}
		}
		//交换inst
		localBestSolution = s.getNewNeiborFromNowSolution(nowSolution)
		instA := localBestSolution.instKV[localBestCandidate.instA]
		instB := localBestSolution.instKV[localBestCandidate.instB]
		//如果inst交换的次数等于3，则不再交换该inst
		if instA.exchanged == 3 || instB.exchanged == 3 {
			continue
		}
		if canSwap, totalScore := s.trySwap(instA, instB, localBestSolution, true); canSwap {
			needBridgeMachine := false
			for i := 0; i < 200; i++ {
				if instB.Machine.Usage[i]+instA.App.Resource[i] > instB.Machine.Capacity[i]{
					needBridgeMachine = true
					break
				}
			}
			//先将instA先放到其他机器上，再将instB放machineA上，最后将instA移到machineB上
			machineA := instA.Machine
			machineB := instB.Machine
			if needBridgeMachine || machineB.hasConflictInst(instA){
				if instA.exchanged >=2 {
					continue
				}
				for _, machine :=range localBestSolution.machines {
					if machine.canPutInst(instA) {
						machine.put(instA)
						submitA := SubmitResult{instA.exchanged, instA.Id, instA.Machine.Id}
						s.submitResult = append(s.submitResult, submitA)
						machineA.put(instB)
						submitB := SubmitResult{instB.exchanged, instB.Id, instB.Machine.Id}
						s.submitResult = append(s.submitResult, submitB)
						machineB.put(instA)
						s.searchResult[localBestCandidate.instA] = localBestCandidate.instB
						submitA = SubmitResult{instA.exchanged, instA.Id, instA.Machine.Id}
						s.submitResult = append(s.submitResult, submitA)
						localBestSolution.totalScore = totalScore
						break
					}
				}
			} else {
				localBestSolution.totalScore = totalScore
				s.doSwap(instA, instB)
				s.searchResult[localBestCandidate.instA] = localBestCandidate.instB
				submitA := SubmitResult{instA.exchanged, instA.Id, instA.Machine.Id}
				s.submitResult = append(s.submitResult, submitA)
				submitB := SubmitResult{instB.exchanged, instB.Id, instB.Machine.Id}
				s.submitResult = append(s.submitResult, submitB)
			}
		}

		// time.Sleep(time.Second)
		//设置局部最优解的特赦值
		if localBestCandidate.totalScore < nowSolution.permitValue {
			localBestSolution.permitValue = localBestCandidate.totalScore
		} else if nowSolution.totalScore < localBestCandidate.permitValue {
			localBestSolution.permitValue = nowSolution.totalScore
		}
		//更新tabulist
		s.updateTabuList() //todo: pair移除tabulist之后要释放候选解
		s.lock.Lock()
		s.tabuList[localBestCandidate.exchangeAppFromA] = TabuLen
		s.tabuList[localBestCandidate.exchangeAppFromB] = TabuLen
		s.tabuList[localBestCandidate.exchangeAppToA] = TabuLen
		s.tabuList[localBestCandidate.exchangeAppToB] = TabuLen
		//如果局部最优解优于当前最优解，则将局部最优解设置为当前最优解
		if localBestCandidate.totalScore < s.bestSolution.totalScore-0.001 {
			fmt.Printf("New bestsolution: %0.8f --> %0.8f\n", s.bestSolution.totalScore, localBestCandidate.totalScore)
			s.bestSolution = nil
			s.bestSolution = s.getNewNeiborFromNowSolution(localBestSolution)
		}
		//更新候选集，即按照localBestCandidate的交换规则交换实例
		//s.updateCandidates(localBestSolution)
		nowSolution = localBestSolution
		fmt.Printf("Local best solution score: %.8f\n", localBestCandidate.totalScore)
		s.nowSolution = nil
		s.nowSolution = s.getNewNeiborFromNowSolution(localBestSolution)
		s.lock.Unlock()
	}
}

func (s *Scheduler) updateTabuList() {
	s.lock.Lock()
	defer s.lock.Unlock()
	for key := range s.tabuList {
		s.tabuList[key]--
		if s.tabuList[key] == 0 {
			delete(s.tabuList, key)
		}
	}
}

func (s *Scheduler) getInitNeiborSet(nowSolution *Solution) {
	// s.getNewNeiborFromNowSolution(0, s.bestSolution)
	// s.getNewNeiborFromNowSolution(1, nowSolution)
	s.solutions[0] = s.bestSolution
	s.solutions[1] = nowSolution
	s.solutions = s.solutions[:2]
	for i := 0; i < InitNeiborSize; i++ {
		machineAIndex := rand.Intn(len(s.bestSolution.machines))
		machineBIndex := rand.Intn(len(s.bestSolution.machines))
		// s.getNewNeibor(i) //生成第i个邻居
		//newSolve := s.getNewNeiborFromNowSolution(nowSolution)
		newNeibor := new(Candidate)
		newNeibor.totalScore = 1e9
		s.neibors = append(s.neibors, newNeibor)
		// fmt.Printf("solve %d score: %0.2f\n", i, TotalScore(s.solutions[i-1].machines))
		swaped := false
		for _, instA := range s.solutions[0].machines[machineAIndex].appKV {
			for _, instB := range s.solutions[0].machines[machineBIndex].appKV {
				if instA.App == instB.App {
					continue
				}

				if instB.Machine.Id != s.solutions[0].machines[machineBIndex].Id {
					continue //inst2已经在上一轮循环swap过了
				}
				//如果machineA上的appA已经和machineB上的appB交换过了，则重新生成
				exchangeAppFromA := ExchangeApp{instA.AppId, s.solutions[0].machines[machineAIndex].Id, instB.AppId, s.solutions[0].machines[machineBIndex].Id}
				exchangeAppFromB := ExchangeApp{instB.AppId, s.solutions[0].machines[machineBIndex].Id, instA.AppId, s.solutions[0].machines[machineAIndex].Id}
				exchangeAppToA := ExchangeApp{instA.AppId, s.solutions[0].machines[machineBIndex].Id, instB.AppId, s.solutions[0].machines[machineAIndex].Id}
				exchangeAppToB := ExchangeApp{instB.AppId, s.solutions[0].machines[machineAIndex].Id, instA.AppId, s.solutions[0].machines[machineBIndex].Id}
				duplicate := false
				for _, neibor := range s.neibors{
					if neibor.exchangeAppFromA == exchangeAppFromA || neibor.exchangeAppToA == exchangeAppToA ||
						neibor.exchangeAppFromB == exchangeAppFromB || neibor.exchangeAppToB == exchangeAppToB {
						fmt.Printf("%s had been swaped with %s", exchangeAppFromA, exchangeAppToB)
						duplicate = true
						break
					}
				}
				if duplicate {
					continue
				}
				if canSwap, totalScore := s.trySwap(instA, instB, s.solutions[0], true); canSwap { //交换第i个解里面随机的两个inst
					//s.doSwap(instA, instB)
					s.getNewNeibor(i, instA, instB, machineAIndex, machineBIndex, totalScore)
					fmt.Printf("swap (%s) <-> (%s): %f\n",
						instA.Id, instB.Id, s.neibors[i].totalScore)
					swaped = true
					break //产生一个邻居后，跳到最外层循环，继续产生下一个邻居
				}

			}
			if swaped {
				break
			}
		}
		if !swaped {
			i--
			s.neibors = s.neibors[:len(s.neibors)-1]
		}
	}
}

func (s *Scheduler) getNewNeibor(index int, instA, instB *Instance, machineAIndex, machineBIndex int, totalScore float64) {
	s.neibors[index].instA = instA.Id
	s.neibors[index].instB = instB.Id
	s.neibors[index].totalScore = totalScore
	s.neibors[index].isCandidate = false
	s.neibors[index].permitValue = totalScore
	s.neibors[index].exchangeAppFromA = ExchangeApp{instA.AppId, s.solutions[0].machines[machineAIndex].Id, instB.AppId, s.solutions[0].machines[machineBIndex].Id}
	s.neibors[index].exchangeAppFromB = ExchangeApp{instB.AppId, s.solutions[0].machines[machineBIndex].Id, instA.AppId, s.solutions[0].machines[machineAIndex].Id}
	s.neibors[index].exchangeAppToA = ExchangeApp{instA.AppId, s.solutions[0].machines[machineBIndex].Id, instB.AppId, s.solutions[0].machines[machineAIndex].Id}
	s.neibors[index].exchangeAppToB = ExchangeApp{instB.AppId, s.solutions[0].machines[machineAIndex].Id, instA.AppId, s.solutions[0].machines[machineBIndex].Id}
}

func (s *Scheduler) getNewNeiborFromNowSolution(nowSolution *Solution) *Solution {
	nowSolution.lock.Lock()
	defer nowSolution.lock.Unlock()
	s1 := *nowSolution
	appKV := make(map[string]*Application)
	for _, app := range s1.appKV {
		app2 := &Application{
			Id:        app.Id,
			Cpu:       app.Cpu,
			Mem:       app.Mem,
			Disk:      app.Disk,
			Instances: app.Instances,
			Resource:  app.Resource,
		}
		appKV[app2.Id] = app2
	}
	instKV := make(map[string]*Instance)
	for _, inst := range s1.instKV {
		inst1 := &Instance{
			Id:       inst.Id,
			App:      appKV[inst.AppId],
			AppId:    inst.AppId,
			MachineId: inst.MachineId,
			deployed: false,
			exchanged: inst.exchanged,
		}
		instKV[inst1.Id] = inst1
	}
	machineKV := make(map[string]*Machine)
	var machines []*Machine
	for _, machine := range s1.machines {
		m2 := &Machine{
			Id:              machine.Id,
			CpuCapacity:     machine.CpuCapacity,
			MemCapacity:     machine.MemCapacity,
			DiskCapacity:    machine.DiskCapacity,
			PmpCapacity:     machine.PmpCapacity,
			Capacity:        machine.Capacity,
			Usage:           machine.Usage,
			instKV:          make(map[string]*Instance),
			appKV:           make(map[string]*Instance),
			appCntKV:        machine.appCntKV,
			appInterference: machine.appInterference,
		}
		for instid := range machine.instKV {
			m2.instKV[instid] = instKV[instid]
		}
		for appid, inst := range machine.appKV {
			m2.appKV[appid] = instKV[inst.Id]
		}
		machines = append(machines, m2)
		machineKV[m2.Id] = m2
	}
	for _, inst := range s1.instKV {
		if inst.Machine != nil {
			instKV[inst.Id].Machine = machineKV[inst.Machine.Id]
			instKV[inst.Id].deployed = true
		}
	}
	totalScore := TotalScore(machines)
	s2 := &Solution{
		machines:    machines,
		instKV:      instKV,
		appKV:       appKV,
		machineKV:   machineKV,
		totalScore:  totalScore,
		permitValue: s1.permitValue,
	}
	return s2
}

func (s *Scheduler) getCandidateNeibor() {
	// scoreMapToNeibor := make(map[float64]int)
	// for i, solution := range s.solutions {
	// 	scoreMapToNeibor[solution.totalScore] = i
	// }
	neibors := NeiborSlice(s.neibors[:])
	sort.Sort(neibors)
	s.candidates = neibors[:CandidateLen]
	for _, candidate := range s.candidates {
		candidate.isCandidate = true
	}
}

func (s *Scheduler) permitValue(nowBestSolution *Solution) float64 {
	return nowBestSolution.totalScore
}

//searchFile: search file;  index: the indexth neighbor of the neighbor set, index=0 for original solution
func (s *Scheduler) readSearchFile(searchFile string, index int) {
	if _, err := os.Open(searchFile); err!= nil {
		return
	}
	lines := ReadLines(searchFile)
	for _, line := range lines {
		split := strings.Split(line, ",")
		instA := s.solutions[index].instKV[split[0]]
		instB := s.solutions[index].instKV[split[1]]
		s.doSwap(instA, instB)
	}
}

func (s *Scheduler) trySwap(inst1, inst2 *Instance, solution *Solution, force bool) (bool, float64) {
	m1 := inst1.Machine
	m2 := inst2.Machine
	if m1.Id == m2.Id {
		return false, 0
	}

	if m1.Id > m2.Id { //令m1总是Id较小的机器，即可避免死锁
		m1 = inst2.Machine
		m2 = inst1.Machine
	}
	m1.lock.Lock()
	m2.lock.Lock()
	defer m2.lock.Unlock()
	defer m1.lock.Unlock()
	for i := 0; i < 200; i++ {
		if inst1.Machine.Usage[i]-inst1.App.Resource[i]+inst2.App.Resource[i] > inst1.Machine.Capacity[i] {
			return false, 0
		}
		if inst2.Machine.Usage[i]-inst2.App.Resource[i]+inst1.App.Resource[i] > inst2.Machine.Capacity[i] {
			return false, 0
		}
	}
	if hasConflict(inst1, inst2) || hasConflict(inst2, inst1) {
		return false, 0
	}

	var cpu1 [98]float64
	var cpu2 [98]float64
	machineCpu1 := inst1.Machine.cpuUsage()
	machineCpu2 := inst2.Machine.cpuUsage()
	for i := 0; i < 98; i++ {
		cpu1[i] = machineCpu1[i] - inst1.App.Cpu[i] + inst2.App.Cpu[i]
		cpu2[i] = machineCpu2[i] - inst2.App.Cpu[i] + inst1.App.Cpu[i]
	}
	score1 := cpuScore(cpu1[0:98], inst1.Machine.CpuCapacity, len(inst1.Machine.instKV))
	score2 := cpuScore(cpu2[0:98], inst2.Machine.CpuCapacity, len(inst2.Machine.instKV))
	delta := score1 + score2 - (inst1.Machine.score() + inst2.Machine.score())

	if !force && (delta > 0 || -delta < 0.0001) {
		return false, 0
	}

	//solution.totalScore += delta
	//s.doSwap(inst1, inst2)
	return true, solution.totalScore + delta
}

func hasConflict(inst1, inst2 *Instance) bool {
	m := inst1.Machine
	instCnt := m.appCntKV[inst1.AppId]
	if instCnt == 1 {
		delete(m.appCntKV, inst1.AppId)
	} else {
		m.appCntKV[inst1.AppId] = instCnt - 1
	}

	result := m.hasConflictInst(inst2)
	m.appCntKV[inst1.AppId] = instCnt //恢复原状
	return result
}


func (s *Scheduler) doSwap(inst1, inst2 *Instance) {
	machine1 := inst1.Machine
	machine2 := inst2.Machine
	machine1.put(inst2)
	machine2.put(inst1)
}

func (s *Scheduler) output() {
	//machines := MachineSlice(s.bestSolution.machines)
	//sort.Sort(machines)
	//usedMachine := 0
	//for _, machine := range machines {
	//	if machine.diskUsage() != 0 {
	//		usedMachine++
	//	}
	//}
	filePath := fmt.Sprintf("search-result/search_file")
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		os.Create(filePath)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for key, value := range s.searchResult{
		line := fmt.Sprintf("%s,%s", key, value)
		fmt.Fprintln(w, line)
	}
	w.Flush()
	fmt.Printf("writing to %s\n", filePath)

	filePath = "submit_file.csv"
	f, err = os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		os.Open(filePath)
	}
	defer f.Close()
	w = bufio.NewWriter(f)
	for _, submit := range s.submitResult {
		line := fmt.Sprintf("%d,%s,%s", submit.round, submit.instance, submit.machine)
		fmt.Fprintln(w, line)
	}
	w.Flush()
	fmt.Printf("writing to %s\n", filePath)
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: go run main.go <search_file> <cores>")
		os.Exit(1)
	}

	rand.Seed(1)

	searchFile := os.Args[1]
	cores, _ := strconv.Atoi(os.Args[2])

	runtime.GOMAXPROCS(cores)

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, os.Kill) //todo: 让goroutine正常停止

	machines, instKV, appKV, machineKV := ReadData()
	scheduler := NewScheduler(searchFile, machines, instKV, appKV, machineKV)
	//scheduler.getInitNeiborSet(scheduler.bestSolution)
	//scheduler.getCandidateNeibor()
	fmt.Println(TotalScore(scheduler.solutions[0].machines))

	for i := 0; i < cores; i++ {
		go scheduler.tabuSearch()
		// go scheduler.search()
	}
	// scheduler.tabuSearch()
	<-stopChan
	// for k, _ := range scheduler.initIndex {
	// 	fmt.Println(k)
	// }
	scheduler.output()
	fmt.Printf("total score: %.6f\n", scheduler.bestSolution.totalScore)
}
