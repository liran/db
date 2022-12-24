package db

type ListOption struct {
	Begin        string // The starting key, not included by default
	ContainBegin bool   // The result contains the key of begin
	Reverse      bool   // Iterate from back to front
	Limit        int    // The maximum number of iterations
	KeyOnly      bool   // Only iterate over keys
}
