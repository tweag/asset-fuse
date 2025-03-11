package grpcheaderinterceptor

import (
	"context"
	"net/url"
	"strings"

	"github.com/tweag/asset-fuse/auth/credential"
	"github.com/tweag/asset-fuse/internal/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type authenticatingInterceptor struct {
	helper credential.Helper
}

// unaryAddHeaders injects headers into a unary gRPC call.
func (i *authenticatingInterceptor) unaryAddHeaders(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	}

	md = addCredentialsToMD(ctx, cc.Target(), method, md, i.helper)

	return invoker(ctx, method, req, reply, cc, opts...)
}

// streamAddHeaders injects headers into a stream gRPC call.
func (i *authenticatingInterceptor) streamAddHeaders(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	}

	md = addCredentialsToMD(ctx, cc.Target(), method, md, i.helper)

	return streamer(ctx, desc, cc, method, opts...)
}

func addCredentialsToMD(ctx context.Context, target, method string, md metadata.MD, helper credential.Helper) metadata.MD {
	hostname, ok := strings.CutPrefix(target, "dns:")
	if !ok {
		logging.Warningf("authenticating gRPC: unknown target definition %s", target)
		return md
	}

	methodParts := strings.Split(method, "/")
	if len(methodParts) < 2 || len(methodParts[0]) != 0 {
		logging.Warningf("authenticating gRPC: unknown method definition %s", method)
		return md
	}

	u := url.URL{
		Scheme: "https",
		Host:   hostname,
		Path:   "/" + methodParts[1],
	}
	headers, _, err := helper.Get(ctx, u.String())
	if err != nil {
		logging.Warningf("authenticating gRPC: failed to get credentials for %s: %v", u.String(), err)
		return md
	}
	if len(headers) == 0 {
		logging.Debugf("authenticating gRPC: credential helper found no headers for %s - trying unauthenticated connection", u.String())
		return md
	}

	for k, vs := range headers {
		md.Append(k, vs...)
	}
	return md
}

func DialOptions(helper credential.Helper) []grpc.DialOption {
	interceptor := &authenticatingInterceptor{helper: helper}
	return []grpc.DialOption{
		grpc.WithUnaryInterceptor(interceptor.unaryAddHeaders),
		grpc.WithStreamInterceptor(interceptor.streamAddHeaders),
	}
}
