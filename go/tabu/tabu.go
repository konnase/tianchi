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
	"github.com/sirupsen/logrus"
)

const (
	MachineInput         = "data/scheduling_semifinal_data_20180815/machine_resources.%s.csv"
	InstanceInput        = "data/scheduling_semifinal_data_20180815/instance_deploy.%s.csv"
	ApplicationInput     = "data/scheduling_semifinal_data_20180815/app_resources.csv"
	AppInterferenceInput = "data/scheduling_semifinal_data_20180815/app_interference.csv"
	JobInfoInput         = "data/scheduling_semifinal_data_20180815/job_info.%s.csv"
	SearchMachineRange   = 5000
	InitNeiborSize       = 70
	CandidateLen         = 60
	TabuLen              = 4
)

type Instance struct {
	Id      string
	App     *Application
	AppId   string
	Machine *Machine
	MachineId string

	deployed bool
	exchanged bool
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
	inst.exchanged = true

	m.instKV[inst.Id] = inst
	m.appKV[inst.App.Id] = inst //每类应用只记录一个实例用来swap即可

	if _, ok := m.appCntKV[inst.App.Id]; ok {
		m.appCntKV[inst.App.Id] += 1
	} else {
		m.appCntKV[inst.App.Id] = 1
	}
}

func (m *Machine) noCascatePut(inst *Instance) {
	if _, ok := m.instKV[inst.Id]; ok {
		return
	}

	for i := 0; i < 200; i++ {
		m.Usage[i] += inst.App.Resource[i]
	}

	inst.Machine = m
	inst.deployed = true
	inst.exchanged = true

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

	for _, newInst := range m.instKV {
		if newInst.App.Id == inst.App.Id {
			m.appKV[inst.App.Id] = newInst
			break
		}
	}
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
			num += 1
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

func ReadData(dataset string) ([]*Machine, map[string]*Instance, map[string]*Application, map[string]*Machine) {
	interference := ReadAppInterference(ReadLines(AppInterferenceInput))
	machineInput := fmt.Sprintf(MachineInput, dataset)
	machines := ReadMachine(ReadLines(machineInput), interference)
	machineKV := make(map[string]*Machine)
	for _, machine := range machines {
		machineKV[machine.Id] = machine
	}

	apps := ReadApplication(ReadLines(ApplicationInput))
	appKV := make(map[string]*Application)
	for _, app := range apps {
		appKV[app.Id] = app
	}

	instanceInput := fmt.Sprintf(InstanceInput, dataset)
	insts := ReadInstance(ReadLines(instanceInput))
	instKV := make(map[string]*Instance)
	for _, inst := range insts {
		inst.App = appKV[inst.AppId]
		inst.Machine = machineKV[inst.MachineId]
		inst.Machine.put(inst)
		inst.exchanged = false
		instKV[inst.Id] = inst
	}

	return machines, instKV, appKV, machineKV
}

//machineA上的appA移动到machineB上去
type ExchangeApp struct {
	appA     string
	machineA string
	machineB string
}

type SubmitResult struct {
	round int
	instance string
	machine string
}

type Candidate struct {
	instA string
	machineA string
	machineB string

	totalScore float64
	isCandidate bool
	permitValue float64

	moveAppFromMachineAToB ExchangeApp //记录这个解的移动的方向
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
	initSolution    *Solution
	neibors []*Candidate
	candidates   []*Candidate

	unchangedSolution *Solution  //在移动inst的过程中，不将inst从其原来的machine上删除
	bestSolution *Solution //固定位置solutions[0]
	currentSolution  *Solution //固定位置solutions[1]

	tabuList        map[ExchangeApp]int //禁忌表
	searchResult map[string]string
	submitResult []SubmitResult
	pickFrom map[string]string

	dataset string
	submitFile string
	round int
	lock       sync.RWMutex
}

func NewScheduler(dataset, submitFile string, machine []*Machine, instKV map[string]*Instance, appKV map[string]*Application, machineKV map[string]*Machine) *Scheduler {
	solution := &Solution{
		instKV:      instKV,
		appKV:       appKV,
		machineKV:   machineKV,
		machines:    machine,
	}
	scheduler := &Scheduler{
		initSolution: solution,
		dataset: dataset,
		submitFile: submitFile,
		tabuList:   make(map[ExchangeApp]int),
		searchResult: make(map[string]string),
		pickFrom: make(map[string]string),
	}
	scheduler.initSolution.totalScore = TotalScore(scheduler.initSolution.machines) //原始解的分数
	scheduler.initSolution.permitValue = scheduler.initSolution.totalScore
	scheduler.unchangedSolution = scheduler.getNewSolutionFromCurrentSolution(scheduler.initSolution)

	scheduler.round = scheduler.readSubmitFile(submitFile)

	scheduler.bestSolution = scheduler.getNewSolutionFromCurrentSolution(scheduler.initSolution)
	scheduler.currentSolution = scheduler.getNewSolutionFromCurrentSolution(scheduler.bestSolution)


	return scheduler
}

func (s *Scheduler) tabuSearch() {
	s.startSearch()
}

func (s *Scheduler) startSearch() {

	for round := 1; round <= 3; round ++ {
		if s.round == -1 {
			round = 1
		}else{
			round = s.round
		}
		var localBestCandidate *Candidate
		var localBestSolution *Solution
		logrus.Infof("round: %d", round)
		for {
			//得到候选集
			if getInitNeibor := s.getInitNeighbor(s.currentSolution); !getInitNeibor {
				s.prepareNextRound(s.currentSolution)
				break
			}
			s.getCandidateNeibor()
			//初始化候选集
			if len(s.tabuList) == 0 {
				for _, candidateX := range s.candidates {
					candidateX.isCandidate = true
				}
			} else {
				s.lock.Lock()
				for tabu := range s.tabuList {
					for _, candidateX := range s.candidates {
						if tabu == candidateX.moveAppFromMachineAToB {
							candidateX.isCandidate = false
						} else {
							candidateX.isCandidate = true
						}
						if candidateX.totalScore < s.currentSolution.permitValue { //特赦规则
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
			//移动inst
			localBestSolution = s.currentSolution
			instA := localBestSolution.instKV[localBestCandidate.instA]
			uinstA := s.unchangedSolution.instKV[localBestCandidate.instA]
			machineB := localBestSolution.machineKV[localBestCandidate.machineB]
			umachineB := s.unchangedSolution.machineKV[localBestCandidate.machineB]
			//如果inst已经被移动了，则不再移动该inst
			if instA.exchanged  {
				continue
			}
			ucanMove, _ := s.tryMove(uinstA, umachineB, s.unchangedSolution, true)
			canMove, _ := s.tryMove(instA, machineB, localBestSolution, true)
			if  canMove && ucanMove && machineB.canPutInst(instA) && umachineB.canPutInst(uinstA){
				s.pickFrom[uinstA.Id] = uinstA.Machine.Id
				//logrus.Infof("%s can put %s: %t", machineB.Id, instA.Id, machineB.canPutInst(instA))
				umachineB.noCascatePut(uinstA) //todo: uinstA迁移之后居然对instA的迁移产生了影响
				//logrus.Infof("%s can put %s: %t", machineB.Id, instA.Id, machineB.canPutInst(instA))
				machineB.put(instA)
				localBestSolution.totalScore = localBestCandidate.totalScore
				s.unchangedSolution.totalScore = localBestCandidate.totalScore
				submitA := SubmitResult{round, instA.Id, instA.Machine.Id}
				s.submitResult = append(s.submitResult, submitA)

				//设置局部最优解的特赦值
				if localBestCandidate.totalScore < s.currentSolution.permitValue {
					localBestSolution.permitValue = localBestCandidate.totalScore
				} else if s.currentSolution.totalScore < localBestCandidate.permitValue {
					localBestSolution.permitValue = s.currentSolution.totalScore
				}
				//更新tabulist
				s.updateTabuList() //todo: pair移除tabulist之后要释放候选解
				s.lock.Lock()
				s.tabuList[localBestCandidate.moveAppFromMachineAToB] = TabuLen

				//如果局部最优解优于当前最优解，则将局部最优解设置为当前最优解
				if localBestCandidate.totalScore < s.bestSolution.totalScore-0.0001 {
					logrus.Infof("New best solution: %0.8f --> %0.8f\n", s.bestSolution.totalScore, localBestCandidate.totalScore)
					s.bestSolution = nil
					s.bestSolution = s.getNewSolutionFromCurrentSolution(localBestSolution)
				}

				s.currentSolution = localBestSolution
				logrus.Infof("Local best solution score: %.8f\n", localBestCandidate.totalScore)
				//s.s.currentSolution = nil
				//s.s.currentSolution = s.getNewSolutionFromCurrentSolution(localBestSolution)
				s.lock.Unlock()
			}
		}

	}
}

func (s *Scheduler) prepareNextRound(currentSolution *Solution) {
	for _, inst := range s.bestSolution.instKV {
		inst.exchanged = false
	}
	s.unchangedSolution = nil
	s.unchangedSolution = s.getNewSolutionFromCurrentSolution(currentSolution)
	s.pickFrom = nil
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

func (s *Scheduler) getInitNeighbor(currentSolution *Solution) bool{
	failIter := 0
	for i := 0; i < InitNeiborSize; i++ {
		machineAIndex := rand.Intn(len(s.bestSolution.machines))
		machineBIndex := s.getMachineBIndex()
		if machineAIndex == machineBIndex {
			i--
			continue
		}
		newNeibor := new(Candidate)
		newNeibor.totalScore = 1e9
		s.neibors = append(s.neibors, newNeibor)
		moved := false
		machineA := currentSolution.machines[machineAIndex]
		machineB := currentSolution.machines[machineBIndex]
		for _, instA := range machineA.appKV {
			//如果machineA上的appA已经迁移到machineB了，则重新生成
			moveAppFromMachineAToB := ExchangeApp{
				instA.AppId,
				machineA.Id,
				currentSolution.machines[machineBIndex].Id,
			}
			duplicate := false
			for _, neibor := range s.neibors{
				if neibor.moveAppFromMachineAToB == moveAppFromMachineAToB  {
					logrus.Infof("%s had been moved", moveAppFromMachineAToB)
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			if canMove, totalScore := s.tryMove(instA, machineB, currentSolution, true); canMove { //交换第i个解里面随机的两个inst
				s.getNewNeibor(i, instA, machineA, machineB, totalScore)
				//fmt.Printf("move (%s) --> (%s): %f\n", instA.Id, machineB.Id, s.neibors[i].totalScore)
				moved = true
				break //产生一个邻居后，继续产生下一个邻居
			}
		}
		if !moved {
			failIter++
			i--
			s.neibors = s.neibors[:len(s.neibors)-1]
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
	if s.dataset == "c" || s.dataset == "d"{
		rate := rand.Intn(100)
		if rate > 30 {
			machineBIndex = rand.Intn(3000) + 6000
		} else {
			machineBIndex = rand.Intn(6000)
		}
	} else if s.dataset == "e" {
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
	s.neibors[index].instA = instA.Id
	s.neibors[index].machineA = machineA.Id
	s.neibors[index].machineB = machineB.Id
	s.neibors[index].totalScore = totalScore
	s.neibors[index].isCandidate = false
	s.neibors[index].permitValue = totalScore
	s.neibors[index].moveAppFromMachineAToB = ExchangeApp{
		instA.AppId,
		machineA.Id,
		machineB.Id,
	}
}

func (s *Scheduler) getNewSolutionFromCurrentSolution(currentSolution *Solution) *Solution {
	currentSolution.lock.Lock()
	defer currentSolution.lock.Unlock()
	s1 := *currentSolution
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
	s2 := &Solution{
		machines:    machines,
		instKV:      instKV,
		appKV:       appKV,
		machineKV:   machineKV,
		totalScore:  s1.totalScore,
		permitValue: s1.permitValue,
	}
	return s2
}

func (s *Scheduler) getCandidateNeibor() {
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

//submitFile: submit file;
func (s *Scheduler) readSubmitFile(submitFile string) int {
	if _, err := os.Open(submitFile); err!= nil {
		return -1
	}
	lines := ReadLines(submitFile)
	round := 1
	for _, line := range lines {
		split := strings.Split(line, ",")
		if inRound, _ := strconv.Atoi(split[0]); round == inRound{
			inst := s.initSolution.instKV[split[1]]
			machine := s.initSolution.machineKV[split[2]]
			s.pickFrom[inst.Id] = inst.Machine.Id
			machine.put(inst)

			uinst := s.unchangedSolution.instKV[split[1]]
			umachine := s.unchangedSolution.machineKV[split[2]]
			umachine.noCascatePut(uinst)

		} else{
			round++
			s.unchangedSolution = nil
			s.unchangedSolution = s.getNewSolutionFromCurrentSolution(s.initSolution)
			s.pickFrom = nil

			inst := s.initSolution.instKV[split[1]]
			machine := s.initSolution.machineKV[split[2]]
			s.pickFrom[inst.Id] = inst.Machine.Id
			machine.put(inst)

			uinst := s.unchangedSolution.instKV[split[1]]
			umachine := s.unchangedSolution.machineKV[split[2]]
			umachine.noCascatePut(uinst)
		}
	}
	s.initSolution.totalScore = TotalScore(s.initSolution.machines)
	s.initSolution.permitValue = s.initSolution.totalScore
	return round
}

func (s *Scheduler) tryMove(inst *Instance, m2 *Machine, solution *Solution, force bool) (bool, float64) {
	if inst.exchanged {
		return false, 1e9
	}
	m1 := inst.Machine
	if m1.Id == m2.Id {
		return false, 1e9
	}
	m1.lock.Lock()
	m2.lock.Lock()
	defer m2.lock.Unlock()
	defer m1.lock.Unlock()
	if !m2.canPutInst(inst) {
		return false, 1e9
	}

	var cpu1 [98]float64
	var cpu2 [98]float64
	machineCpu1 := m1.cpuUsage()
	machineCpu2 := m2.cpuUsage()
	for i := 0; i < 98; i++ {
		cpu1[i] = machineCpu1[i] - inst.App.Cpu[i]
		cpu2[i] = machineCpu2[i] + inst.App.Cpu[i]
	}
	score1 := cpuScore(cpu1[0:98], m1.CpuCapacity, len(m1.instKV))
	score2 := cpuScore(cpu2[0:98], m2.CpuCapacity, len(m2.instKV))
	delta := score1 + score2 - (m1.score() + m2.score())

	if !force && (delta > 0 || -delta < 0.0001) {
		return false, 1e9
	}

	return true, solution.totalScore + delta
}


func (s *Scheduler) output(dataset string) {

	filePath := fmt.Sprintf("submit_file_%s.csv", dataset)
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, submit := range s.submitResult {
		line := fmt.Sprintf("%d,%s,%s", submit.round, submit.instance, submit.machine)
		//line := fmt.Sprintf("%s,%s", submit.instance, submit.machine)
		fmt.Fprintln(w, line)
	}
	w.Flush()
	fmt.Printf("writing to %s\n", filePath)
}

func main() {
	if len(os.Args) != 4 {
		fmt.Println("Usage: go run main.go <submit_file> <cores> <dataset> <round>")
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
	logrus.Infof("totalScore: %.8f\n",TotalScore(scheduler.initSolution.machines))

	for i := 0; i < cores; i++ {
		go scheduler.tabuSearch()
	}
	<-stopChan
	scheduler.output(dataset)
	fmt.Printf("total score: %.6f\n", scheduler.bestSolution.totalScore)
}
