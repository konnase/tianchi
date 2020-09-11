package scheduler

const (
	MachineInput         = "../data/scheduling_semifinal_data_20180815/machine_resources.%s.csv"
	InstanceInput        = "../data/scheduling_semifinal_data_20180815/instance_deploy.%s.csv"
	ApplicationInput     = "../data/scheduling_semifinal_data_20180815/app_resources.csv"
	AppInterferenceInput = "../data/scheduling_semifinal_data_20180815/app_interference.csv"
	//SearchMachineRange   = 5000
	//JobInfoInput         = "data/scheduling_semifinal_data_20180815/job_info.%s.csv"

	//InitNeighborSize = 3000
	//CandidateLen     = 800
	//AllowScoreDecrease = true  //设置在搜索邻居的时候是否允许分数下降。true时，InitNeighborSize要设置大一些（大于1000）；false时InitNeighborSize要设置小点（小于100）
	InitNeighborSize   = 10
	CandidateLen       = 8
	AllowScoreDecrease = false //设置在搜索邻居的时候是否允许分数下降。true时，InitNeighborSize要设置大一些（大于1000）；false时InitNeighborSize要设置小点（小于100）

	BackSpaceLen = 50 //设置每多少步进行一次undo操作
	SyncIter     = 479
	TabuLen      = 400
)
