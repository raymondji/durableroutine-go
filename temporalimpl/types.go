package temporalimpl

import "time"

// WorkflowInput is the input to StateroutineWorkflow.
type WorkflowInput struct {
	HandlerKey   string       `json:"handlerKey"`
	State        any          `json:"state"`
	QueryResults []QueryEntry `json:"queryResults,omitempty"`
}

// QueryEntry is a query name + result pair preserved across continue-as-new.
type QueryEntry struct {
	QueryName string `json:"queryName"`
	Result    any    `json:"result"`
}

// ActivityInput is passed to the RunHandler activity.
type ActivityInput struct {
	HandlerKey     string `json:"handlerKey"`
	StateroutineID string `json:"stateroutineID"`
	State          any    `json:"state"`
	Message        any    `json:"message,omitempty"`
	Error          string `json:"error,omitempty"`
}

// ActivityOutput is returned by the RunHandler activity.
type ActivityOutput struct {
	Done          bool              `json:"done"`
	Result        any               `json:"result,omitempty"`
	Suspend       *SerializedSuspend `json:"suspend,omitempty"`
	QueryResults  []QueryEntry      `json:"queryResults,omitempty"`
	SpawnRequests []SpawnEntry      `json:"spawnRequests,omitempty"`
	SendRequests  []SendEntry       `json:"sendRequests,omitempty"`
	CallResponse  any               `json:"callResponse,omitempty"`
}

// SerializedSuspend is the wire format for a Suspend value.
type SerializedSuspend struct {
	Cases []SerializedCase `json:"cases"`
}

// SerializedCase is the wire format for a Case.
type SerializedCase struct {
	TimerDuration *time.Duration `json:"timerDuration,omitempty"`
	SendName      string         `json:"sendName,omitempty"`
	CallName      string         `json:"callName,omitempty"`
	Immediate     bool           `json:"immediate,omitempty"`
	State         any            `json:"state"`
	HandlerKey    string         `json:"handlerKey"`
}

// SpawnEntry is a request to start a child stateroutine.
type SpawnEntry struct {
	StateroutineID string `json:"stateroutineID"`
	StateKind      string `json:"stateKind"`
	State          any    `json:"state"`
}

// SendEntry is a request to send a signal to another stateroutine.
type SendEntry struct {
	StateroutineID string `json:"stateroutineID"`
	StateKind      string `json:"stateKind"`
	MsgKind        string `json:"msgKind"`
	Msg            any    `json:"msg"`
}
