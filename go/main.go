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
	"bytes"
)

const (
	MachineInput         = "data/scheduling_preliminary_b_machine_resources_20180726.csv"
	InstanceInput        = "data/scheduling_preliminary_b_instance_deploy_20180726.csv"
	ApplicationInput     = "data/scheduling_preliminary_b_app_resources_20180726.csv"
	AppInterferenceInput = "data/scheduling_preliminary_b_app_interference_20180726.csv"
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

func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
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
		m[splits[0]][splits[1]], _ = strconv.Atoi(splits[2])
	}
	return m
}

func ReadMachine(lines []string, interference AppInterference) []*Machine {
	var m []*Machine
	for _, line := range lines {
		m = append(m, NewMachine(line, interference))
	}
	return m
}

func ReadApplication(lines []string) []*Application {
	var a []*Application
	for _, line := range lines {
		a = append(a, NewApplication(line))
	}
	return a
}

func ReadInstance(lines []string) []*Instance {
	var i []*Instance
	for _, line := range lines {
		i = append(i, NewInstance(line))
	}
	return i
}

func ReadData() ([]*Machine, map[string]*Instance, map[string]*Application) {
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
	return machines, instKV, appKV
}

type Scheduler struct {
	machines []*Machine

	instKV map[string]*Instance
	appKV  map[string]*Application

	totalScore float64 //todo: 共享变量，不影响计算过程，只用于输出
	done       bool
}

func NewScheduler(searchFile string, machine []*Machine, instKV map[string]*Instance, appKV map[string]*Application) *Scheduler {
	scheduler := &Scheduler{
		machines: machine,
		instKV:   instKV,
		appKV:    appKV,
		done:     false,
	}

	lines := ReadLines(searchFile)
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
			scheduler.machines[machineIndex].put(scheduler.instKV[inst])
		}
	}
	scheduler.totalScore = TotalScore(scheduler.machines)
	return scheduler
}

func (s *Scheduler) search(wait *sync.WaitGroup) {
	shuffledIndex := rand.Perm(len(s.machines))

	for _, i := range shuffledIndex {
		for _, j := range shuffledIndex {
			if i == j {
				continue
			}
			for _, inst1 := range s.machines[i].appKV {
				for _, inst2 := range s.machines[j].appKV {
					if s.done {
						fmt.Printf("goroutine %d done\n", getGID())
						wait.Done()
						return
					}
					if inst1.App == inst2.App {
						continue
					}
					if inst2.Machine.Id != s.machines[j].Id {
						continue //inst2已经在上一轮循环swap过了
					}
					if s.trySwap(inst1, inst2) {
						fmt.Printf("swap (%s) <-> (%s): %f\n",
							inst1.Id, inst2.Id, s.totalScore)
						break //inst1也swap过了，外层循环继续下一个实例
					}
				}
			}
		}
	}
}

func (s *Scheduler) trySwap(inst1, inst2 *Instance) bool {
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

	if delta > 0 || -delta < 0.00001 {
		return false
	}

	if hasConflict(inst1, inst2) || hasConflict(inst2, inst1) {
		return false
	}

	s.totalScore += delta
	s.doSwap(inst1, inst2)
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

func (s *Scheduler) Done() {
	s.done = true
}

func (s *Scheduler) output() {
	machines := MachineSlice(s.machines)
	sort.Sort(machines)
	usedMachine := 0
	for _, machine := range machines {
		if machine.diskUsage() != 0 {
			usedMachine++
		}
	}
	filePath := fmt.Sprintf("search-result/search_%s_%dm", strconv.FormatInt(int64(s.totalScore), 10), usedMachine)
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

	machines, instKV, appKV := ReadData()
	scheduler := NewScheduler(searchFile, machines, instKV, appKV)
	fmt.Println(scheduler.totalScore)

	var wg sync.WaitGroup
	for i := 0; i < cores; i++ {
		wg.Add(1)
		go scheduler.search(&wg)
	}

	<-stopChan
	scheduler.Done()
	wg.Wait()
	scheduler.output()
	fmt.Printf("total score: %.6f", TotalScore(scheduler.machines))
}
