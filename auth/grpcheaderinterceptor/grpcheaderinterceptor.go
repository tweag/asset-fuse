package grpcheaderinterceptor

import (
	"context"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// unaryAddHeaders injects headers into a unary gRPC call.
func unaryAddHeaders(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	}

	md = addCredentialsToMD(md)

	return invoker(ctx, method, req, reply, cc, opts...)
}

// streamAddHeaders injects headers into a stream gRPC call.
func streamAddHeaders(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	}

	md = addCredentialsToMD(md)

	return streamer(ctx, desc, cc, method, opts...)
}

func addCredentialsToMD(md metadata.MD) metadata.MD {
	// TODO: use credential helper
	md.Set("x-buildbuddy-api-key", os.Getenv("BUILD_BUDDY_API_KEY"))
	return md
}

func DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithUnaryInterceptor(unaryAddHeaders),
		grpc.WithStreamInterceptor(streamAddHeaders),
	}
}
