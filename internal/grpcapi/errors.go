package grpcapi

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/brunocampos-ssa/portfolio-api/internal/domain"
)

// =============================================================================
// gRPC error mapping
// =============================================================================
//
// REST has writeError (httpapi/response.go) translating AppError → HTTP
// status. gRPC needs the symmetric translation: AppError → status.Error
// with the right google.golang.org/grpc/codes value.
//
// Maintain this table side-by-side with mapCodeToHTTPStatus so the two
// transports stay symmetric. README has the full table.

// toStatus converts any error coming out of the service layer into a
// gRPC status. Untyped errors collapse to Internal — the same fallback
// the REST layer uses.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	var appErr *domain.AppError
	if !errors.As(err, &appErr) {
		return status.Error(codes.Internal, "internal server error")
	}
	return status.Error(codeFor(appErr.Code), appErr.Message)
}

func codeFor(code domain.ErrorCode) codes.Code {
	switch code {
	case domain.CodeNotFound:
		return codes.NotFound
	case domain.CodeValidation, domain.CodeInvalidInput:
		return codes.InvalidArgument
	case domain.CodeUnsupportedNetwork:
		return codes.InvalidArgument
	case domain.CodeUnauthenticated:
		return codes.Unauthenticated
	case domain.CodeForbidden:
		return codes.PermissionDenied
	case domain.CodeConflict:
		return codes.AlreadyExists
	case domain.CodeProviderFailure:
		return codes.Unavailable
	case domain.CodeUpstreamTimeout:
		return codes.DeadlineExceeded
	case domain.CodeInternal:
		return codes.Internal
	default:
		return codes.Internal
	}
}
