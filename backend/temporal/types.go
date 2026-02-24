package temporal

import (
	"encoding/json"
	"time"
)

// WorkflowInput is the input to RoutineWorkflow.
type WorkflowInput struct {
	HandlerKey       string       `json:"handlerKey"`
	Input            json.RawMessage `json:"input"`
	QueryResults     []QueryEntry `json:"queryResults,omitempty"`
	MaxHistoryLength int32        `json:"maxHistoryLength,omitempty"`
}

// QueryEntry is a query name + result pair preserved across continue-as-new.
type QueryEntry struct {
	QueryName string `json:"queryName"`
	Result    json.RawMessage `json:"result"`
}

// ActivityInput is passed to the RunHandler activity.
type ActivityInput struct {
	HandlerKey string `json:"handlerKey"`
	RoutineID  string `json:"routineID"`
	Input      json.RawMessage `json:"input"`
	Message    json.RawMessage `json:"message,omitempty"`
	Error      string `json:"error,omitempty"`
}

// ActivityOutput is returned by the RunHandler activity.
type ActivityOutput struct {
	Done          bool              `json:"done"`
	Result        json.RawMessage   `json:"result,omitempty"`
	Continuation  *SerializedContinuation `json:"continuation,omitempty"`
	QueryResults  []QueryEntry      `json:"queryResults,omitempty"`
	StartRequests []StartEntry      `json:"startRequests,omitempty"`
	SendRequests  []SendEntry       `json:"sendRequests,omitempty"`
	CallResponse  json.RawMessage   `json:"callResponse,omitempty"`
}

// SerializedContinuation is the wire format for a Continuation value.
type SerializedContinuation struct {
	Cases []SerializedCase `json:"cases"`
}

// SerializedCase is the wire format for a Case.
type SerializedCase struct {
	TimerDuration *time.Duration `json:"timerDuration,omitempty"`
	SendName      string         `json:"sendName,omitempty"`
	CallName      string         `json:"callName,omitempty"`
	Immediate     bool           `json:"immediate,omitempty"`
	Input         json.RawMessage `json:"input"`
	HandlerKey    string          `json:"handlerKey"`
}

// StartEntry is a request to start a child routine.
type StartEntry struct {
	RoutineID  string `json:"routineID"`
	InputKind  string `json:"inputKind"`
	ResultKind string `json:"resultKind"`
	Input      json.RawMessage `json:"input"`
}

// SendEntry is a request to send a message to another routine.
type SendEntry struct {
	RoutineID        string          `json:"routineID"`
	InputKind        string          `json:"inputKind"`
	ExternalInputKind string         `json:"externalInputKind"`
	ResultKind       string          `json:"resultKind"`
	Msg              json.RawMessage `json:"msg"`
}
