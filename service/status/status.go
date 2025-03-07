package status

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
	// 	The operation was rejected because the system is not in a state required for the operation’s execution.
	// For example, the directory to be deleted is non-empty, an rmdir operation is applied to a non-directory, etc.
	// Service implementors can use the following guidelines to decide between FAILED_PRECONDITION, ABORTED, and UNAVAILABLE:
	// (a) Use UNAVAILABLE if the client can retry just the failing call.
	// (b) Use ABORTED if the client should retry at a higher level
	// (e.g., when a client-specified test-and-set fails, indicating the client should restart a read-modify-write sequence).
	// (c) Use FAILED_PRECONDITION if the client should not retry until the system state has been explicitly fixed.
	// E.g., if an “rmdir” fails because the directory is non-empty, FAILED_PRECONDITION should be returned since the client
	// should not retry unless the files are deleted from the directory.
	Status_FAILED_PRECONDITION = 9
	// The operation could not be completed, typically due to a failed consistency check.
	Status_ABORTED = 10
	// Internal errors. This means that some invariants expected by the underlying system have been broken.
	// This error code is reserved for serious errors.
	Status_INTERNAL = 13
)
