package asset

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	remoteasset_proto "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	"github.com/tweag/asset-fuse/api"
	"github.com/tweag/asset-fuse/auth/credential"
	"github.com/tweag/asset-fuse/integrity"
	integritypkg "github.com/tweag/asset-fuse/integrity"
	"github.com/tweag/asset-fuse/internal/logging"
	"github.com/tweag/asset-fuse/service/internal/protohelper"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RemoteAssetService uses the remote asset API to access assets via gRPC.
// See also: https://raw.githubusercontent.com/bazelbuild/remote-apis/refs/tags/v2.11.0-rc2/build/bazel/remote/asset/v1/remote_asset.proto
type RemoteAssetService struct {
	client               remoteasset_proto.FetchClient
	helper               credential.Helper
	propagateCredentials bool
}

func NewRemote(target string, helper credential.Helper, propagateCredentials bool, opts ...grpc.DialOption) (*RemoteAssetService, error) {
	conn, err := protohelper.Client(target, helper, opts...)
	if err != nil {
		return nil, err
	}

	return &RemoteAssetService{
		client:               remoteasset_proto.NewFetchClient(conn),
		helper:               helper,
		propagateCredentials: propagateCredentials,
	}, nil
}

func (r *RemoteAssetService) FetchBlob(
	ctx context.Context, timeout time.Duration, oldestContentAccepted time.Time,
	asset api.Asset, digestFunction integrity.Algorithm,
) (FetchBlobResponse, error) {
	if r.propagateCredentials {
		asset.Qualifiers = r.authenticate(ctx, asset)
	}

	resp, err := r.client.FetchBlob(ctx, protoFetchBlobRequest(
		timeout, oldestContentAccepted, asset.URIs, asset.Integrity, asset.Qualifiers, digestFunction,
	))
	if err != nil {
		return FetchBlobResponse{}, err
	}

	out, err := fromProtoFetchBlobResponse(resp)
	if err != nil {
		return out, err
	}

	// perform some basic validation
	if knownChecksum, ok := asset.Integrity.ChecksumForAlgorithm(digestFunction); ok {
		// If the digest is known in advance, we can validate it.
		knownDigest := integritypkg.NewDigest(knownChecksum.Hash, out.BlobDigest.SizeBytes, digestFunction)
		if !knownDigest.Equals(out.BlobDigest, digestFunction) {
			return FetchBlobResponse{}, fmt.Errorf("remote asset api: FetchBlob returned an unexpected digest expected %s, got %s", knownDigest.Hex(digestFunction), out.BlobDigest.Hex(digestFunction))
		}
	}

	return out, nil
}

func (r *RemoteAssetService) authenticate(ctx context.Context, asset api.Asset) map[string]string {
	updatedQualifiers := maps.Clone(asset.Qualifiers)

	// we query the credential helper for this asset
	// and add the headers to the qualifiers
	for i, url := range asset.URIs {
		headers, _, err := r.helper.Get(ctx, url)
		if err != nil {
			logging.Warningf("getting credentials for %s: %v", url, err)
		}
		if len(headers) > 0 && updatedQualifiers == nil {
			updatedQualifiers = make(map[string]string)
		}
		for k, v := range headers {
			// This is a Bazel-specific convention for specifying HTTP headers per URL.
			// https://www.rfc-editor.org/rfc/rfc9110.html#name-field-order permits
			// merging the field-values with a comma.
			updatedQualifiers[fmt.Sprintf("http_header_url:%d:%s", i, k)] = strings.Join(v, ",")
		}
	}
	return updatedQualifiers
}

func protoFetchBlobRequest(
	timeout time.Duration, oldestContentAccepted time.Time,
	uris []string, integrity integrity.Integrity, qualifiers map[string]string,
	digestFunction integrity.Algorithm,
) *remoteasset_proto.FetchBlobRequest {
	req := &remoteasset_proto.FetchBlobRequest{
		Uris:           uris,
		DigestFunction: protohelper.ProtoDigestFunction(digestFunction),
	}
	if timeout != 0 {
		req.Timeout = durationpb.New(timeout)
	}
	if !oldestContentAccepted.IsZero() {
		req.OldestContentAccepted = timestamppb.New(oldestContentAccepted)
	}

	// we need to merge integrity and qualifiers a list of unique qualifiers
	uniqueQualifiers := make(map[string]string)
	maps.Copy(uniqueQualifiers, qualifiers)

	// In theory, we would like to send all checksums we know and let the server
	// decide which one to use:
	// uniqueQualifiers["checksum.sri"] = integrity.ToSRIString()
	// After looking at concrete implementations of the remote asset API,
	// it seems that sending only the sri for the digest function is most widely supported.
	// If that's not available, we try them all (with hardcoded preference).
	checksum, ok := integrity.BestSingleChecksum(digestFunction)
	if !ok {
		// we should never get here.
		// if we do, fix the bug.
		// TODO: maybe handle this gracefully before v1.0.0.
		// TODO: it may even be fine to allow this case,
		//       as long as we the user explicitly doesn't care about determinism via some flag.
		panic("no checksum found in integrity")
	}
	uniqueQualifiers["checksum.sri"] = checksum.ToSRI()

	for k, v := range uniqueQualifiers {
		req.Qualifiers = append(req.Qualifiers, &remoteasset_proto.Qualifier{
			Name:  k,
			Value: v,
		})
	}

	return req
}

func fromProtoFetchBlobResponse(resp *remoteasset_proto.FetchBlobResponse) (FetchBlobResponse, error) {
	if resp == nil {
		return FetchBlobResponse{}, errors.New("FetchBlobResponse is nil")
	}
	digest, err := integrity.DigestFromHex(resp.BlobDigest.Hash, resp.BlobDigest.SizeBytes, protohelper.FromProtoDigestFunction(resp.DigestFunction))
	if err != nil {
		return FetchBlobResponse{}, err
	}
	return FetchBlobResponse{
		Status:         protohelper.FromProtoStatus(resp.Status),
		URI:            resp.Uri,
		Qualifiers:     fromProtoQualifiers(resp.Qualifiers),
		ExpiresAt:      resp.ExpiresAt.AsTime(),
		BlobDigest:     digest,
		DigestFunction: protohelper.FromProtoDigestFunction(resp.DigestFunction),
	}, nil
}

func fromProtoQualifiers(qualifiers []*remoteasset_proto.Qualifier) map[string]string {
	m := make(map[string]string, len(qualifiers))
	for _, q := range qualifiers {
		m[q.Name] = q.Value
	}
	return m
}

var _ Asset = &RemoteAssetService{}
