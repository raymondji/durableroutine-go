package durablecore

func HandlerKey(kind, resultKind string) string {
	return "handler:" + kind + ":" + resultKind
}
func SendKey(inputKind, externalInputKind, resultKind string) string {
	return "send:" + inputKind + ":" + externalInputKind + ":" + resultKind
}
func CallKey(inputKind, externalReqKind, externalRespKind, resultKind string) string {
	return "call:" + inputKind + ":" + externalReqKind + ":" + externalRespKind + ":" + resultKind
}
func ErrorKey(primaryKey string) string { return "error:" + primaryKey }
