package api

type Status struct {
	Code    StatusCode
	Message string
}

type StatusCode int32

const (
	// The operation completed successfully.
	Status_OK StatusCode = 0
	// Unknown error.
	// For example, this error may be returned when a Status value received
	// from another address space belongs to an error space that is not known
	// in this address space.
	// Also errors raised by APIs that do not return enough error information
	// may be converted to this error.
	Status_UNKNOWN = 2
	// The operation could not be completed within the specified timeout.
	Status_DEADLINE_EXCEEDED = 4
	// The requested asset was not found at the specified location.
	Status_NOT_FOUND = 5
	// The request was rejected by a remote server, or requested an asset from a disallowed origin.
	Status_PERMISSION_DENIED = 7
	// There is insufficient quota of some resource to perform the requested operation. The client may retry after a delay.
	Status_RESOURCE_EXHAUSTED = 8
	// The operation could not be completed, typically due to a failed consistency check.
	Status_ABORTED = 10
)
