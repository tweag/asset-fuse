package protohelper

import (
	"fmt"
	"strings"

	remoteexecution_proto "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/tweag/asset-fuse/auth/grpcheaderinterceptor"
	"github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/service/status"
	gstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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

func Client(uri string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts = append([]grpc.DialOption{}, opts...)

	schemeAndRest := strings.SplitN(uri, "://", 2)
	if len(schemeAndRest) != 2 {
		return nil, fmt.Errorf("invalid uri for grpc: %s", uri)
	}
	switch schemeAndRest[0] {
	case "grpc":
		// unencrypted grpc
		// TODO: maybe this should be guarded by a flag?
	case "grpcs":
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(nil)))
	default:
		return nil, fmt.Errorf("unsupported scheme for grpc: %s", schemeAndRest[0])
	}

	target := fmt.Sprintf("dns:%s", schemeAndRest[1])

	opts = append(opts, grpcheaderinterceptor.DialOptions()...)

	return grpc.NewClient(target, opts...)
}
