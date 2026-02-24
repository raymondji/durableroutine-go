package durablecore

func HandlerKey(kind string) string            { return "handler:" + kind }
func SendKey(stateKind, msgKind string) string  { return "send:" + stateKind + ":" + msgKind }
func CallKey(stateKind, reqKind string) string  { return "call:" + stateKind + ":" + reqKind }
func ErrorKey(primaryKey string) string         { return "error:" + primaryKey }
