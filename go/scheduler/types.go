package scheduler


import (
	"strconv"
	"strings"
	"sync"
	"math"
)

const (
	MachineInput         = "data/scheduling_semifinal_data_20180815/machine_resources.%s.csv"
	InstanceInput        = "data/scheduling_semifinal_data_20180815/instance_deploy.%s.csv"
	ApplicationInput     = "data/scheduling_semifinal_data_20180815/app_resources.csv"
	AppInterferenceInput = "data/scheduling_semifinal_data_20180815/app_interference.csv"
	JobInfoInput         = "data/scheduling_semifinal_data_20180815/job_info.%s.csv"
	SearchMachineRange   = 5000
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

	Deployed bool
	Exchanged bool
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

	InstKV          map[string]*Instance
	AppCntKV        map[string]int
	AppKV           map[string]*Instance //<appId, onlyOneInstance>
	AppInterference AppInterference

	Lock sync.Mutex
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
		InstKV:          make(map[string]*Instance),
		AppCntKV:        make(map[string]int),
		AppKV:           make(map[string]*Instance),
		AppInterference: interference,
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

func (m *Machine) CpuUsage() []float64 {
	return m.Usage[0:98]
}

func (m *Machine) MemUsage() []float64 {
	return m.Usage[99:196]
}

func (m *Machine) DiskUsage() float64 {
	return m.Usage[196]
}

func (m *Machine) PmpUsage() []float64 {
	return m.Usage[197:200]
}

func (m *Machine) IsCpuOverload() bool {
	for _, v := range m.CpuUsage() {
		if v > m.CpuCapacity {
			return true
		}
	}
	return false
}

func (m *Machine) IsMemOverload() bool {
	for _, v := range m.MemUsage() {
		if v > m.MemCapacity {
			return true
		}
	}
	return false
}

func (m *Machine) IsDiskOverload() bool {
	return m.DiskUsage() > m.DiskCapacity
}

func (m *Machine) IsPmpOverload() bool {
	for i, v := range m.PmpUsage() {
		if v > m.PmpCapacity[i] {
			return true
		}
	}
	return false
}

func (m *Machine) IsOverload() bool {
	return m.IsCpuOverload() || m.IsMemOverload() || m.IsDiskOverload() || m.IsPmpOverload()
}

func (m *Machine) Score() float64 {
	if m.DiskUsage() == 0 {
		return 0.0
	}
	return CpuScore(m.CpuUsage(), m.CpuCapacity, len(m.InstKV))
}

func CpuScore(cpuUsage []float64, cpuCapacity float64, instNum int) float64 {
	score := 0.0
	for _, v := range cpuUsage {
		util := v / cpuCapacity
		alpha := float64(instNum + 1)
		score += 1 + alpha*(math.Exp(math.Max(util-0.5, 0))-1)

	}
	return score / 98.0
}

func (m *Machine) Put(inst *Instance) {
	if _, ok := m.InstKV[inst.Id]; ok {
		return
	}

	for i := 0; i < 200; i++ {
		m.Usage[i] += inst.App.Resource[i]
	}

	if inst.Machine != nil {
		inst.Machine.Remove(inst)
	}

	inst.Machine = m
	inst.Deployed = true
	inst.Exchanged = true

	m.InstKV[inst.Id] = inst
	m.AppKV[inst.App.Id] = inst //每类应用只记录一个实例用来swap即可

	if _, ok := m.AppCntKV[inst.App.Id]; ok {
		m.AppCntKV[inst.App.Id] += 1
	} else {
		m.AppCntKV[inst.App.Id] = 1
	}
}

func (m *Machine) NoCascatePut(inst *Instance) {
	if _, ok := m.InstKV[inst.Id]; ok {
		return
	}

	for i := 0; i < 200; i++ {
		m.Usage[i] += inst.App.Resource[i]
	}

	inst.Machine = m
	inst.Deployed = true
	inst.Exchanged = true

	m.InstKV[inst.Id] = inst
	m.AppKV[inst.App.Id] = inst //每类应用只记录一个实例用来swap即可

	if _, ok := m.AppCntKV[inst.App.Id]; ok {
		m.AppCntKV[inst.App.Id] += 1
	} else {
		m.AppCntKV[inst.App.Id] = 1
	}
}

func (m *Machine) Remove(inst *Instance) {
	if _, ok := m.InstKV[inst.Id]; !ok {
		return
	}

	for i := 0; i < 200; i++ {
		m.Usage[i] -= inst.App.Resource[i]
	}

	inst.Machine = nil
	inst.Deployed = false

	delete(m.InstKV, inst.Id)

	m.AppCntKV[inst.App.Id] -= 1

	if m.AppCntKV[inst.App.Id] == 0 {
		delete(m.AppCntKV, inst.App.Id)
		delete(m.AppKV, inst.App.Id)
	} else {
		for _, newInst := range m.InstKV {
			if newInst.App.Id == inst.App.Id {
				m.AppKV[inst.App.Id] = newInst
				break
			}
		}
	}

}

func (m *Machine) CanPutInst(inst *Instance) bool {
	return !m.HasConflictInst(inst) && !m.OutOfCapacityInst(inst)
}

func (m *Machine) OutOfCapacityInst(inst *Instance) bool {
	for i := 0; i < 200; i++ {
		if inst.App.Resource[i]+m.Usage[i] > m.Capacity[i] {
			return true
		}
	}
	return false
}

func (m *Machine) HasConflictInst(inst *Instance) bool {
	appNow := inst.App.Id
	appNowCnt := 0
	if _, ok := m.AppCntKV[appNow]; ok {
		appNowCnt = m.AppCntKV[appNow]
	}


	for appId, appCnt := range m.AppCntKV {
		//if appId == "app_2485" {
		//	logrus.Infof("%s %s appCnt: %d  limit: %d", appId, appNow, appCnt, m.ConflictLimitOf(appId, appNow))
		//}
		if appNowCnt+1 > m.ConflictLimitOf(appId, appNow) {
			return true
		}
		if appCnt > m.ConflictLimitOf(appNow, appId) {
			return true
		}
	}
	return false
}

func (m *Machine) ConflictLimitOf(appIdA, appIdB string) int {
	if _, ok := m.AppInterference[appIdA][appIdB]; ok {
		return m.AppInterference[appIdA][appIdB]
	}
	return 1e9
}

func (m *Machine) HasConflict() bool {
	cnt := 0
	for appIdA, appCntA := range m.AppCntKV {
		for appIdB, appCntB := range m.AppCntKV {
			if appCntB > m.ConflictLimitOf(appIdA, appIdB) {
				cnt += 1
			}
			if appCntA > m.ConflictLimitOf(appIdB, appIdA) {
				cnt += 1
			}
		}
	}
	return cnt > 0
}

func (m *Machine) InstDiskList() string {
	var disks []string
	for _, inst := range m.InstKV {
		disks = append(disks, strconv.FormatInt(int64(inst.App.Disk), 10))
	}
	return strings.Join(disks, ",")
}

func (m *Machine) InstIdList() string {
	var ids []string
	for _, inst := range m.InstKV {
		ids = append(ids, inst.Id)
	}
	return strings.Join(ids, ",")
}

//MachineA上的AppA移动到MachineB上去
type ExchangeApp struct {
	AppA     string
	MachineA string
	MachineB string
}

type SubmitResult struct {
	Round int
	Instance string
	Machine string
}

type Candidate struct {
	InstA string
	MachineA string
	MachineB string

	TotalScore float64
	IsCandidate bool
	PermitValue float64

	MoveAppFromMachineAToB ExchangeApp //记录这个解的移动的方向
}
type Solution struct {
	Machines    []*Machine
	InstKV      map[string]*Instance
	AppKV       map[string]*Application
	MachineKV   map[string]*Machine
	TotalScore  float64 //todo: 共享变量，不影响计算过程，只用于输出
	PermitValue float64

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
	return s[i].TotalScore < s[j].TotalScore
}
