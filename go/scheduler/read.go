package scheduler

import (
	"fmt"
	"bufio"
	"io"
	"strings"
	"strconv"
	"os"
)

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
		inst.Machine.Put(inst)
		inst.Exchanged = false
		instKV[inst.Id] = inst
	}

	return machines, instKV, appKV, machineKV
}
