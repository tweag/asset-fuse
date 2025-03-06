package protohelper

import (
	remoteexecution_proto "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/service/status"
	gstatus "google.golang.org/genproto/googleapis/rpc/status"
)

func ProtoDigestFunction(digestFunction integrity.Algorithm) remoteexecution_proto.DigestFunction_Value {
	switch digestFunction {
	case integrity.SHA256:
		return remoteexecution_proto.DigestFunction_SHA256
	case integrity.SHA384:
		return remoteexecution_proto.DigestFunction_SHA384
	case integrity.SHA512:
		return remoteexecution_proto.DigestFunction_SHA512
	case integrity.Blake3:
		return remoteexecution_proto.DigestFunction_BLAKE3
	}
	return remoteexecution_proto.DigestFunction_UNKNOWN
}

func FromProtoDigestFunction(digestFunction remoteexecution_proto.DigestFunction_Value) integrity.Algorithm {
	switch digestFunction {
	case remoteexecution_proto.DigestFunction_SHA256:
		return integrity.SHA256
	case remoteexecution_proto.DigestFunction_SHA384:
		return integrity.SHA384
	case remoteexecution_proto.DigestFunction_SHA512:
		return integrity.SHA512
	case remoteexecution_proto.DigestFunction_BLAKE3:
		return integrity.Blake3
	}
	// TODO: is this correct?
	// It should be safe, since a collision between two
	// hash functions we trust is negligible.
	return integrity.SHA256
}

func FromProtoStatus(googleStatus *gstatus.Status) status.Status {
	return status.Status{
		Code:    status.StatusCode(googleStatus.Code),
		Message: googleStatus.Message,
	}
}
