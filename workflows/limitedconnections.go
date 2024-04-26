package workflows

type LimitedConnectionsWorkflow struct {
	w   Workflow
	sem chan struct{}
}

func NewLimitedConnectionsWorkflow(workflow Workflow, limit int) *LimitedConnectionsWorkflow {
	return &LimitedConnectionsWorkflow{
		w:   workflow,
		sem: make(chan struct{}, limit),
	}
}

func (w *LimitedConnectionsWorkflow) Execute() {
	w.sem <- struct{}{}
	defer func() { <-w.sem }()
	w.w.Execute()
}
