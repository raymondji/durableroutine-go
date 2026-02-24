package durablecore

func HandlerKey(kind, resultKind string) string {
	return "handler:" + kind + ":" + resultKind
}
func SendKey(stateKind, msgKind, resultKind string) string {
	return "send:" + stateKind + ":" + msgKind + ":" + resultKind
}
func CallKey(stateKind, reqKind, respKind, resultKind string) string {
	return "call:" + stateKind + ":" + reqKind + ":" + respKind + ":" + resultKind
}
func ErrorKey(primaryKey string) string { return "error:" + primaryKey }
