package utils

//TernaryOp works as a simple ternary operator
func TernaryOp(cond bool, whenTrue interface{}, whenFalse interface{}) interface{} {
	if cond {
		return whenTrue
	}
	return whenFalse
}
