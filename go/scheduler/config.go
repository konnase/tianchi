package scheduler

const (
	MachineInput         = "data/scheduling_semifinal_data_20180815/machine_resources.%s.csv"
	InstanceInput        = "data/scheduling_semifinal_data_20180815/instance_deploy.%s.csv"
	ApplicationInput     = "data/scheduling_semifinal_data_20180815/app_resources.csv"
	AppInterferenceInput = "data/scheduling_semifinal_data_20180815/app_interference.csv"
	//JobInfoInput         = "data/scheduling_semifinal_data_20180815/job_info.%s.csv"
	//SearchMachineRange   = 5000
	InitNeighborSize = 3000
	CandidateLen     = 800
	BackSpaceLen     = 50    //设置每多少步进行一次undo操作
	SyncIter         = 499
	TabuLen          = 400
)
