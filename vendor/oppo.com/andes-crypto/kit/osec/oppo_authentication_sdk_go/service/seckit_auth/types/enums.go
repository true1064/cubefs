package types

type SecurityLevel int

const (
	High SecurityLevel = iota
	Middle
	Low
)

const (
	LocationInvalid = iota
	LocationLocal
	LocationRemote
)
