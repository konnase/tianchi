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
	MachineInput         = "data/scheduling_preliminary_b_machine_resources_20180726.csv"
	InstanceInput        = "data/scheduling_preliminary_b_instance_deploy_20180726.csv"
	ApplicationInput     = "data/scheduling_preliminary_b_app_resources_20180726.csv"
	AppInterferenceInput = "data/scheduling_preliminary_b_app_interference_20180726.csv"
	InitNeiborSet        = "data/init_neibor_set.csv"
	InitNeiborSize       = 20
	TabuLen              = 3
)

type Instance struct {
	Id      string
	App     *Application
	AppId   string
	Machine *Machine

	deployed bool
	//lock     sync.Mutex
}

func NewInstance(line string) *Instance {
	line = strings.TrimSpace(line)
	splits := strings.Split(line, ",")
	return &Instance{
		Id:    splits[0],
		AppId: splits[1],
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
	// machine := new(Machine)
	// machine.Id = splits[0]
	// machine.instKV = make(map[string]*Instance)
	// machine.appCntKV = make(map[string]int)
	// machine.appKV = make(map[string]*Instance)
	// machine.appInterference = interference
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
	return cpuScore(m.cpuUsage(), m.CpuCapacity)
}

func cpuScore(cpuUsage []float64, cpuCapacity float64) float64 {
	score := 0.0
	for _, v := range cpuUsage {
		util := v / cpuCapacity
		score += 1 + 10*(math.Exp(math.Max(util-0.5, 0))-1)
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

	apps := ReadApplication(ReadLines(ApplicationInput))
	appKV := make(map[string]*Application)
	for _, app := range apps {
		appKV[app.Id] = app
	}

	insts := ReadInstance(ReadLines(InstanceInput))
	instKV := make(map[string]*Instance)
	for _, inst := range insts {
		inst.App = appKV[inst.AppId]
		instKV[inst.Id] = inst
	}
	machineKV := make(map[string]*Machine)
	for _, machine := range machines {
		machineKV[machine.Id] = machine
	}
	return machines, instKV, appKV, machineKV
}

type ExchangeApp struct {
	appA     string
	machineA string
	appB     string
	machineB string
}
type Solution struct {
	machines    []*Machine
	instKV      map[string]*Instance
	appKV       map[string]*Application
	machineKV   map[string]*Machine
	totalScore  float64 //todo: 共享变量，不影响计算过程，只用于输出
	isCandidate bool
	permitValue float64

	exchangeAppFromA ExchangeApp //记录这个解的移动的方向
	exchangeAppFromB ExchangeApp //记录这个解的移动的方向
	exchangeAppToA   ExchangeApp //记录这个解的移动的方向
	exchangeAppToB   ExchangeApp //记录这个解的移动的方向
}

type SolutionSlice []*Solution

func (s SolutionSlice) Len() int {
	return len(s)
}

func (s SolutionSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s SolutionSlice) Less(i, j int) bool {
	return s[i].totalScore < s[j].totalScore
}

type Scheduler struct {
	solutions    []*Solution
	candidates   []*Solution
	bestSolution *Solution //固定位置solutions[0]
	nowSolution  *Solution //固定位置solutions[1]

	tabuList        map[ExchangeApp]int //禁忌表
	candidateSetLen int                 //取机器数目的一半

	searchFile string
}

func NewScheduler(searchFile string, machine []*Machine, instKV map[string]*Instance, appKV map[string]*Application, machineKV map[string]*Machine) *Scheduler {
	solution := &Solution{
		instKV:      instKV,
		appKV:       appKV,
		machineKV:   machineKV,
		machines:    machine,
		isCandidate: false,
	}
	scheduler := &Scheduler{
		searchFile: searchFile,
		tabuList:   make(map[ExchangeApp]int),
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
	// s.getInitNeiborSet()
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
	var localBestSolution *Solution
	for {
		//得到候选集
		s.getInitNeiborSet(nowSolution)
		s.getCandidateNeibor()
		//初始化候选集
		if len(s.tabuList) == 0 {
			for _, candidateX := range s.candidates {
				candidateX.isCandidate = true
			}
		} else {
			for tabu := range s.tabuList {
				for _, candidateX := range s.candidates {
					if tabu == candidateX.exchangeAppFromA || tabu == candidateX.exchangeAppToA ||
						tabu == candidateX.exchangeAppFromB || tabu == candidateX.exchangeAppToB {
						candidateX.isCandidate = false
					} else {
						candidateX.isCandidate = true
					}
					if candidateX.totalScore < nowSolution.permitValue { //特赦规则
						candidateX.isCandidate = true
					}
				}
			}
		}
		//从候选集中选择分数最低的解
		i := 0
		for _, candidateS := range s.candidates {
			// println(candidateS.isCandidate)
			if candidateS.isCandidate {
				if i == 0 {
					localBestSolution = candidateS
				}
				if candidateS.totalScore < localBestSolution.totalScore {
					localBestSolution = candidateS
				}
				i++
			}
		}
		//设置局部最优解的特赦值
		if localBestSolution.totalScore < nowSolution.permitValue {
			localBestSolution.permitValue = localBestSolution.totalScore
		} else if nowSolution.totalScore < localBestSolution.permitValue {
			localBestSolution.permitValue = nowSolution.totalScore
		}
		//更新tabulist
		s.updateTabuList() //todo: pair移除tabulist之后要释放候选解
		s.tabuList[localBestSolution.exchangeAppFromA] = TabuLen
		s.tabuList[localBestSolution.exchangeAppFromB] = TabuLen
		s.tabuList[localBestSolution.exchangeAppToA] = TabuLen
		s.tabuList[localBestSolution.exchangeAppToB] = TabuLen
		//如果局部最优解由于当前最优解，则将局部最优解设置为当前最优解
		if localBestSolution.totalScore < s.bestSolution.totalScore {
			fmt.Printf("New bestsolution: %0.4f --> %0.4f\n", s.bestSolution.totalScore, localBestSolution.totalScore)
			s.bestSolution = localBestSolution
		}
		//更新候选集，即按照localBestSolution的交换规则交换实例
		// s.updateCandidates(localBestSolution)
		nowSolution = localBestSolution
	}
}

func (s *Scheduler) updateTabuList() {
	for key := range s.tabuList {
		s.tabuList[key]--
		if s.tabuList[key] == 0 {
			delete(s.tabuList, key)
		}
	}
}

func (s *Scheduler) updateCandidates(localBestSolution *Solution) {
	appA := localBestSolution.exchangeAppFromA.appA
	appB := localBestSolution.exchangeAppFromA.appB
	machine1 := localBestSolution.exchangeAppFromA.machineA
	machine2 := localBestSolution.exchangeAppFromA.machineB
	for _, candidate := range s.candidates {
		if candidate == localBestSolution {
			continue
		}
		if machine1 == candidate.exchangeAppFromA.machineA {
			//如果candidate跟局部最优交换的实例在一台机器上，且实例也一样，则把candidate上已经交换到的candidate.machineB上的instA与machineB上的instB交换
			if appA == candidate.exchangeAppFromA.appA { //machine3的appA与machine2的appB交换
				machine3 := candidate.exchangeAppFromA.machineB
				s.swapCandidate(candidate, appA, appB, machine1, machine2, machine3)
			}
		} else if machine1 == candidate.exchangeAppFromA.machineB {
			if appA == candidate.exchangeAppFromA.appB { //machine3的appA与machine2的appB交换
				machine3 := candidate.exchangeAppFromA.machineA
				s.swapCandidate(candidate, appA, appB, machine1, machine2, machine3)
			}
		} else if machine2 == candidate.exchangeAppFromA.machineA {
			if appB == candidate.exchangeAppFromA.appA { //machine3的appB与machine1的appA交换
				machine3 := candidate.exchangeAppFromA.machineB
				s.swapCandidate(candidate, appB, appA, machine1, machine2, machine3)
			}
		} else if machine2 == candidate.exchangeAppFromA.machineB {
			if appB == candidate.exchangeAppFromA.appB { //machine3的appB与machine1的appA交换
				machine3 := candidate.exchangeAppFromA.machineA
				s.swapCandidate(candidate, appB, appA, machine1, machine2, machine3)
			}
		} else {
			fmt.Println(candidate.machineKV[machine1].appKV[appA], candidate.machineKV[machine2].appKV[appB])
			canswap := s.trySwap(candidate.machineKV[machine1].appKV[appA], candidate.machineKV[machine2].appKV[appB], candidate, true)
			fmt.Println(canswap)
			if canswap {
				s.doSwap(candidate.machineKV[machine1].appKV[appA], candidate.machineKV[machine2].appKV[appB])
				fmt.Printf("candidate swap (%s) <-> (%s): %f\n",
					appA, appB, candidate.totalScore)
			}

		}
	}
}

func (s *Scheduler) swapCandidate(candidate *Solution, appA string, appB string, machine1 string, machine2 string, machine3 string) {
	canswap := s.trySwap(candidate.machineKV[machine3].appKV[appA], candidate.machineKV[machine2].appKV[appB], candidate, true)
	fmt.Println(canswap)
	if canswap {
		s.doSwap(candidate.machineKV[machine3].appKV[appA], candidate.machineKV[machine2].appKV[appB])
		appC := candidate.exchangeAppFromA.appB
		candidate.exchangeAppFromA = ExchangeApp{appB, machine1, appC, machine3}
		candidate.exchangeAppFromB = ExchangeApp{appC, machine3, appB, machine1}
		candidate.exchangeAppToA = ExchangeApp{appB, machine3, appC, machine1}
		candidate.exchangeAppToB = ExchangeApp{appC, machine1, appB, machine3}
		fmt.Printf("candidate swap (%s) <-> (%s): %f\n",
			appA, appB, candidate.totalScore)
	}
}

func (s *Scheduler) getInitNeiborSet(nowSolution *Solution) {
	s.getNewNeiborFromNowSolution(0, s.bestSolution)
	s.getNewNeiborFromNowSolution(1, nowSolution)
	s.solutions = s.solutions[:2]
	for i := 2; i < InitNeiborSize+2; i++ {
		machineAIndex := rand.Intn(len(s.bestSolution.machines))
		machineBIndex := rand.Intn(len(s.bestSolution.machines))
		// s.getNewNeibor(i) //生成第i个邻居
		s.getNewNeiborFromNowSolution(i, nowSolution)
		// fmt.Printf("solve %d score: %0.2f\n", i, TotalScore(s.solutions[i-1].machines))
		swaped := false
		for _, instA := range s.solutions[i].machines[machineAIndex].appKV {
			for _, instB := range s.solutions[i].machines[machineBIndex].appKV {
				if instA.App == instB.App {
					continue
				}
				//如果machineA上的appA已经和machineB上的appB交换过了，则重新生成
				exchangeAppFromA := ExchangeApp{instA.AppId, s.solutions[i].machines[machineAIndex].Id, instB.AppId, s.solutions[i].machines[machineBIndex].Id}
				exchangeAppFromB := ExchangeApp{instB.AppId, s.solutions[i].machines[machineBIndex].Id, instA.AppId, s.solutions[i].machines[machineAIndex].Id}
				exchangeAppToA := ExchangeApp{instA.AppId, s.solutions[i].machines[machineBIndex].Id, instB.AppId, s.solutions[i].machines[machineAIndex].Id}
				exchangeAppToB := ExchangeApp{instB.AppId, s.solutions[i].machines[machineAIndex].Id, instA.AppId, s.solutions[i].machines[machineBIndex].Id}
				if s.solutions[i].exchangeAppFromA == exchangeAppFromA || s.solutions[i].exchangeAppToA == exchangeAppToA ||
					s.solutions[i].exchangeAppFromB == exchangeAppFromB || s.solutions[i].exchangeAppToB == exchangeAppToB {
					fmt.Printf("%s had been swaped with %s", exchangeAppFromA, exchangeAppToB)
					continue
				}
				if instB.Machine.Id != s.solutions[i].machines[machineBIndex].Id {
					continue //inst2已经在上一轮循环swap过了
				}
				if s.trySwap(instA, instB, s.solutions[i], true) { //交换第i个解里面随机的两个inst
					s.doSwap(instA, instB)
					s.solutions[i].exchangeAppFromA = ExchangeApp{instA.AppId, s.solutions[i].machines[machineAIndex].Id, instB.AppId, s.solutions[i].machines[machineBIndex].Id}
					s.solutions[i].exchangeAppFromB = ExchangeApp{instB.AppId, s.solutions[i].machines[machineBIndex].Id, instA.AppId, s.solutions[i].machines[machineAIndex].Id}
					s.solutions[i].exchangeAppToA = ExchangeApp{instA.AppId, s.solutions[i].machines[machineBIndex].Id, instB.AppId, s.solutions[i].machines[machineAIndex].Id}
					s.solutions[i].exchangeAppToB = ExchangeApp{instB.AppId, s.solutions[i].machines[machineAIndex].Id, instA.AppId, s.solutions[i].machines[machineBIndex].Id}
					fmt.Printf("swap (%s) <-> (%s): %f\n",
						instA.Id, instB.Id, s.solutions[i].totalScore)
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
			s.solutions = s.solutions[:len(s.solutions)-1]
		}
	}
}

func (s *Scheduler) getNewNeiborFromNowSolution(index int, nowSolution *Solution) {
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
			deployed: false,
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
		instKV[inst.Id].Machine = machineKV[inst.Machine.Id]
		instKV[inst.Id].deployed = true
	}
	totalScore := TotalScore(machines)
	s2 := &Solution{
		machines:    machines,
		instKV:      instKV,
		appKV:       appKV,
		machineKV:   machineKV,
		totalScore:  totalScore,
		isCandidate: false,
		permitValue: totalScore,
	}
	s.solutions = append(s.solutions, s2)
}

func (s *Scheduler) getNewNeibor(index int) {
	s.solutions = append(s.solutions, new(Solution))
	machines, instKV, appKV, machineKV := ReadData()
	s.solutions[index].instKV = instKV
	s.solutions[index].appKV = appKV
	s.solutions[index].machineKV = machineKV
	s.solutions[index].machines = machines
	s.readSearchFile(s.searchFile, index)
	s.solutions[index].totalScore = TotalScore(s.solutions[index].machines)
	s.solutions[index].permitValue = s.solutions[index].totalScore
	s.solutions[index].isCandidate = false
}

func (s *Scheduler) getCandidateNeibor() {
	candidateNum := 20 //supposed to be scheduler.candidateSetLen
	// scoreMapToNeibor := make(map[float64]int)
	// for i, solution := range s.solutions {
	// 	scoreMapToNeibor[solution.totalScore] = i
	// }
	solutions := SolutionSlice(s.solutions[2:])
	sort.Sort(solutions)
	s.candidates = solutions[:candidateNum]
	for _, candidate := range s.candidates {
		candidate.isCandidate = true
	}
}

func (s *Scheduler) permitValue(nowBestSolution *Solution) float64 {
	return nowBestSolution.totalScore
}

//searchFile: search file;  index: the indexth neighbor of the neighbor set, index=0 for original solution
func (s *Scheduler) readSearchFile(searchFile string, index int) {
	lines := ReadLines(searchFile)
	count := 0
	for i, line := range lines {
		var machineIndex int
		if i < 3000 {
			machineIndex = i + 3000
		} else {
			machineIndex = i - 3000
		}

		split := strings.Split(line, " ")
		str := strings.TrimRight(strings.TrimLeft(split[2], "("), ")")
		insts := strings.Split(str, ",")

		for _, inst := range insts {
			s.solutions[index].machines[machineIndex].put(s.solutions[index].instKV[inst])
		}
		count++
	}
	if index == 0 {
		s.candidateSetLen = count / 2
	}
}

func (s *Scheduler) search() {
	shuffledIndex := rand.Perm(len(s.solutions[0].machines))

	for _, i := range shuffledIndex {
		for _, j := range shuffledIndex {
			if i == j {
				continue
			}
			for _, inst1 := range s.solutions[0].machines[i].appKV {
				for _, inst2 := range s.solutions[0].machines[j].appKV {
					if inst1.App == inst2.App {
						continue
					}
					if inst2.Machine.Id != s.solutions[0].machines[j].Id {
						continue //inst2已经在上一轮循环swap过了
					}
					if s.trySwap(inst1, inst2, s.solutions[0], false) {
						s.doSwap(inst1, inst2)
						fmt.Printf("swap (%s) <-> (%s): %f\n",
							inst1.Id, inst2.Id, s.solutions[0].totalScore)
						break //inst1也swap过了，外层循环继续下一个实例
					}
				}
			}
		}
	}
}

func (s *Scheduler) trySwap(inst1, inst2 *Instance, solution *Solution, force bool) bool {
	m1 := inst1.Machine
	m2 := inst2.Machine
	if m1.Id == m2.Id {
		return false
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
			return false
		}
		if inst2.Machine.Usage[i]-inst2.App.Resource[i]+inst1.App.Resource[i] > inst2.Machine.Capacity[i] {
			return false
		}
	}

	var cpu1 [98]float64
	var cpu2 [98]float64
	machineCpu1 := inst1.Machine.cpuUsage()
	machineCpu2 := inst2.Machine.cpuUsage()
	for i := 0; i < 98; i++ {
		cpu1[i] = machineCpu1[i] - inst1.App.Cpu[i] + inst2.App.Cpu[i]
		cpu2[i] = machineCpu2[i] - inst2.App.Cpu[i] + inst1.App.Cpu[i]
	}
	score1 := cpuScore(cpu1[0:98], inst1.Machine.CpuCapacity)
	score2 := cpuScore(cpu2[0:98], inst2.Machine.CpuCapacity)
	delta := score1 + score2 - (inst1.Machine.score() + inst2.Machine.score())

	if !force && (delta > 0 || -delta < 0.0001) {
		return false
	}

	if hasConflict(inst1, inst2) || hasConflict(inst2, inst1) {
		return false
	}

	solution.totalScore += delta
	return true
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
	machines := MachineSlice(s.bestSolution.machines)
	sort.Sort(machines)
	usedMachine := 0
	for _, machine := range machines {
		if machine.diskUsage() != 0 {
			usedMachine++
		}
	}
	filePath := fmt.Sprintf("search-result/search_%s_%dm", strconv.FormatInt(int64(s.bestSolution.totalScore), 10), usedMachine)
	f, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, machine := range machines {
		if machine.diskUsage() == 0 {
			continue
		}
		line := fmt.Sprintf("total(%.6f,%d): {%s} (%s)", machine.score(), int(machine.diskUsage()), machine.instDiskList(), machine.instIdList())
		fmt.Fprintln(w, line)
	}
	w.Flush()
	fmt.Printf("writing to %s\n", filePath)

	// f, err = os.Create(InitNeiborSet)
	// if err != nil {
	// 	panic(err)
	// }
	// defer f.Close()
	// w = bufio.NewWriter(f)
	// for _, solution := range s.solutions {
	// 	machines := MachineSlice(solution.machines)
	// 	sort.Sort(machines)
	// 	for _, machine := range machines {
	// 		if machine.diskUsage() == 0 {
	// 			continue
	// 		}
	// 		str := fmt.Sprintf("total(%.6f,%d): {%s} (%s)", machine.score(), int(machine.diskUsage()), machine.instDiskList(), machine.instIdList())
	// 		fmt.Fprintln(w, str)
	// 	}
	// 	fmt.Fprintln(w, "#")
	// }
	// w.Flush()
	// fmt.Printf("writing to %s\n", InitNeiborSet)
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
	fmt.Println(scheduler.solutions[0].totalScore)

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
	fmt.Printf("total score: %.6f\n", TotalScore(scheduler.bestSolution.machines))
}
