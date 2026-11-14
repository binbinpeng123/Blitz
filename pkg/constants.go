package pkg

const (
	JobPhasePending      string = "Pending"
	JobPhaseQueuing      string = "Queuing"
	JobPhaseInitializing string = "Initializing"
	JobPhaseRunning      string = "Running"
	JobPhaseTerminating  string = "Terminating"
	JobPhaseStopped      string = "Stopped"
	JobPhaseSucceeded    string = "Succeeded"
	JobPhaseFailed       string = "Failed"
)

// JobPriority 任务优先级
const (
	Low     string = "low"
	Medium  string = "medium"
	High    string = "high"
	Urgent  string = "urgent"
	Highest string = "highest"
)

const (
	InfraComputingLabelKey = "infra-computing"
	InfraComputingLabelVal = "v2"
)

const (
	TrainJob       = "train"
	DataProcessJob = "data-process"
	FillBackJob    = "fillback"
)

func JobPriorityRank(p string) int {
	switch p {
	case Highest:
		return 5
	case Urgent:
		return 4
	case High:
		return 3
	case Medium:
		return 2
	case Low:
		return 1
	default:
		return 1
	}
}
