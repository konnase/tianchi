package scheduler

func TotalScore(machines []*Machine) float64 {
	s := 0.0
	for _, m := range machines {
		s += m.GetScore()
	}
	return s
}

func CopySolution(solution *Solution) *Solution {
	s1 := *solution
	appKV := make(map[string]*Application)
	for _, app := range s1.AppKV {
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
	for _, inst := range s1.InstKV {
		inst1 := &Instance{
			Id:       inst.Id,
			App:      appKV[inst.AppId],
			AppId:    inst.AppId,
			MachineId: inst.MachineId,
			Deployed: false,
			Exchanged: inst.Exchanged,
		}
		instKV[inst1.Id] = inst1
	}
	machineKV := make(map[string]*Machine)
	var machines []*Machine
	for _, machine := range s1.Machines {
		m2 := &Machine{
			Id:              machine.Id,
			CpuCapacity:     machine.CpuCapacity,
			MemCapacity:     machine.MemCapacity,
			DiskCapacity:    machine.DiskCapacity,
			PmpCapacity:     machine.PmpCapacity,
			Capacity:        machine.Capacity,
			Usage:           machine.Usage,
			Score:           -1e9,
			InstKV:          make(map[string]*Instance),
			AppKV:           make(map[string]*Instance),
			AppCntKV:        make(map[string]int),
			AppInterference: machine.AppInterference,
		}
		for instid := range machine.InstKV {
			m2.InstKV[instid] = instKV[instid]
		}
		for appid, inst := range machine.AppKV {
			m2.AppKV[appid] = instKV[inst.Id]
		}
		for appId, cnt := range machine.AppCntKV {
			m2.AppCntKV[appId] = cnt
		}
		machines = append(machines, m2)
		machineKV[m2.Id] = m2
	}
	for _, inst := range s1.InstKV {
		if inst.Machine != nil {
			instKV[inst.Id].Machine = machineKV[inst.Machine.Id]
			instKV[inst.Id].Deployed = true
		}
	}
	s2 := &Solution{
		Machines:    machines,
		InstKV:      instKV,
		AppKV:       appKV,
		MachineKV:   machineKV,
		TotalScore:  s1.TotalScore,
		PermitValue: s1.PermitValue,
	}
	return s2
}
