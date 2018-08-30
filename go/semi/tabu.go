package semi

import (
	"os"
	"fmt"
	"math/rand"
	"strconv"
	"runtime"
	"os/signal"
	"sort"
	"bufio"
)

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
	var localBestSolution *Solution
	round := 0
	for {
		// //得到候选集
		// s.getInitNeiborSet(nowSolution)
		// s.getCandidateNeibor()
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
					localBestSolution = candidateS
				}
				if candidateS.totalScore < localBestSolution.totalScore {
					localBestSolution = candidateS
				}
				i++
			}
		}
		// time.Sleep(time.Second)
		//设置局部最优解的特赦值
		if localBestSolution.totalScore < nowSolution.permitValue {
			localBestSolution.permitValue = localBestSolution.totalScore
		} else if nowSolution.totalScore < localBestSolution.permitValue {
			localBestSolution.permitValue = nowSolution.totalScore
		}
		//更新tabulist
		s.updateTabuList() //todo: pair移除tabulist之后要释放候选解
		s.lock.Lock()
		s.tabuList[localBestSolution.exchangeAppFromA] = TabuLen
		s.tabuList[localBestSolution.exchangeAppFromB] = TabuLen
		s.tabuList[localBestSolution.exchangeAppToA] = TabuLen
		s.tabuList[localBestSolution.exchangeAppToB] = TabuLen
		//如果局部最优解优于当前最优解，则将局部最优解设置为当前最优解
		if localBestSolution.totalScore < s.bestSolution.totalScore-0.001 {
			fmt.Printf("New bestsolution: %0.8f --> %0.8f\n", s.bestSolution.totalScore, localBestSolution.totalScore)
			s.bestSolution = nil
			s.bestSolution = s.getNewNeiborFromNowSolution(localBestSolution)
		}
		//更新候选集，即按照localBestSolution的交换规则交换实例
		s.updateCandidates(localBestSolution)
		nowSolution = localBestSolution
		fmt.Printf("Local best solution score: %.8f\n", localBestSolution.totalScore)
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
				s.swapCandidate(candidate, appB, appA, machine2, machine1, machine3)
			}
		} else if machine2 == candidate.exchangeAppFromA.machineB {
			if appB == candidate.exchangeAppFromA.appB { //machine3的appB与machine1的appA交换
				machine3 := candidate.exchangeAppFromA.machineA
				s.swapCandidate(candidate, appB, appA, machine2, machine1, machine3)
			}
		} else {
			// fmt.Println(candidate.machineKV[machine1].appKV[appA], candidate.machineKV[machine2].appKV[appB])
			// canswap := s.trySwap(candidate.machineKV[machine1].appKV[appA], candidate.machineKV[machine2].appKV[appB], candidate, true)
			// fmt.Println(canswap)
			if candidate.machineKV[machine1].appKV[appA] != nil && candidate.machineKV[machine2].appKV[appB] != nil {
				s.tabuTrySwap(candidate.machineKV[machine1].appKV[appA], candidate.machineKV[machine2].appKV[appB], candidate)
			}
			// fmt.Printf("candidate swap (%s) <-> (%s): %f\n",
			// 	appA, appB, candidate.totalScore)
			// if canswap {
			// }

		}
	}
}

func (s *Scheduler) swapCandidate(candidate *Solution, appA string, appB string, machine1 string, machine2 string, machine3 string) {
	// canswap := s.trySwap(candidate.machineKV[machine3].appKV[appA], candidate.machineKV[machine2].appKV[appB], candidate, true)
	// fmt.Println(canswap)
	if candidate.machineKV[machine3].appKV[appA] != nil && candidate.machineKV[machine2].appKV[appB] != nil {
		s.tabuTrySwap(candidate.machineKV[machine3].appKV[appA], candidate.machineKV[machine2].appKV[appB], candidate)
		appC := candidate.exchangeAppFromA.appB
		candidate.exchangeAppFromA = ExchangeApp{appB, machine1, appC, machine3}
		candidate.exchangeAppFromB = ExchangeApp{appC, machine3, appB, machine1}
		candidate.exchangeAppToA = ExchangeApp{appB, machine3, appC, machine1}
		candidate.exchangeAppToB = ExchangeApp{appC, machine1, appB, machine3}
		fmt.Printf("candidate swap (%s) <-> (%s): %f\n",
			appA, appB, candidate.totalScore)
	}
	// if canswap {
	// }
}

func (s *Scheduler) getInitNeiborSet(nowSolution *Solution) {
	// s.getNewNeiborFromNowSolution(0, s.bestSolution)
	// s.getNewNeiborFromNowSolution(1, nowSolution)
	s.solutions[0] = s.bestSolution
	s.solutions[1] = nowSolution
	s.solutions = s.solutions[:2]
	for i := 2; i < InitNeiborSize+2; i++ {
		machineAIndex := rand.Intn(len(s.bestSolution.machines))
		machineBIndex := rand.Intn(len(s.bestSolution.machines))
		// s.getNewNeibor(i) //生成第i个邻居
		newSolve := s.getNewNeiborFromNowSolution(nowSolution)
		s.solutions = append(s.solutions, newSolve)
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
					// s.doSwap(instA, instB)
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
		isCandidate: false,
		permitValue: totalScore,
	}
	return s2
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
	candidateNum := CandidateLen //supposed to be scheduler.candidateSetLen
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
						// s.doSwap(inst1, inst2)
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

func (s *Scheduler) tabuTrySwap(inst1, inst2 *Instance, solution *Solution) {
	m1 := inst1.Machine
	m2 := inst2.Machine
	if m1.Id > m2.Id { //令m1总是Id较小的机器，即可避免死锁
		m1 = inst2.Machine
		m2 = inst1.Machine
	}
	m1.lock.Lock()
	m2.lock.Lock()
	defer m2.lock.Unlock()
	defer m1.lock.Unlock()
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
	solution.totalScore += delta

	s.doSwap(inst1, inst2)
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
	scheduler.getInitNeiborSet(scheduler.bestSolution)
	scheduler.getCandidateNeibor()
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
	fmt.Printf("nowSolution total score: %.6f\n", TotalScore(scheduler.nowSolution.machines))
}