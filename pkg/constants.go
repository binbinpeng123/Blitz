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

const (
	TrainJob       = "train"
	DataProcessJob = "data-process"
	FillBackJob    = "fillback"
)
