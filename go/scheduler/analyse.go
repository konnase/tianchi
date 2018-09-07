package scheduler

import (
	"bufio"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
)

func (s *Scheduler) Analyse() {
	var machinesCpu []*MachineCpuUtil
	for _, machine := range s.InitSol.Machines {
		// if machine.DiskUsage() < 1e-5 {
		// 	continue
		// }
		machineCpu := new(MachineCpuUtil)

		var instCpuKV = make(map[string]float64)
		maxCpuUtil := 0.0
		minCpuUtil := 1.0e9
		averageCpuUtil := 0.0
		for _, cpuUtil := range machine.CpuUsage() {
			averageCpuUtil += cpuUtil
			if cpuUtil > maxCpuUtil {
				maxCpuUtil = cpuUtil
			}
			if cpuUtil < minCpuUtil {
				minCpuUtil = cpuUtil
			}
		}
		averageCpuUtil /= 98
		averageCpuUtil /= machine.CpuCapacity
		maxCpuUtil /= machine.CpuCapacity
		minCpuUtil /= machine.CpuCapacity
		for _, inst := range machine.InstKV {
			meanCpuUtil := 0.0
			for _, cpuUtil := range inst.App.Cpu {
				meanCpuUtil += cpuUtil
			}
			meanCpuUtil /= 98
			instCpuKV[inst.Id] = meanCpuUtil
		}
		logrus.Infof("%s %f %f", machine.Id, machine.GetScore(), averageCpuUtil)
		machineCpu.MachineId = machine.Id
		machineCpu.MaxCpu = maxCpuUtil
		machineCpu.MinCpu = minCpuUtil
		machineCpu.AverageCpu = averageCpuUtil
		machineCpu.InstCpuKV = instCpuKV
		machinesCpu = append(machinesCpu, machineCpu)
	}
	s.OutPutAnalyse(machinesCpu)
}

func (s *Scheduler) OutPutAnalyse(machinesCpu []*MachineCpuUtil) {
	filePath := fmt.Sprintf("analyse/analyse_%s.csv", s.DataSet)
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	w := bufio.NewWriter(f)
	start := fmt.Sprintf("Machine,Average,MaxCpu,MinCpu,InstCpu")
	fmt.Fprintln(w, start)
	for _, machineCpu := range machinesCpu {
		line := fmt.Sprintf("%s,%.4f,%.4f,%.4f", machineCpu.MachineId, machineCpu.AverageCpu, machineCpu.MaxCpu, machineCpu.MinCpu)
		allInst := ""
		for instId, cpuUtil := range machineCpu.InstCpuKV {
			allInst += fmt.Sprintf("%s:%.4f,", instId, cpuUtil)
		}
		lineWithInst := fmt.Sprintf("%s,%s", line, allInst)
		fmt.Fprintln(w, lineWithInst)
	}
	w.Flush()
}
