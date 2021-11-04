package server

import (
	"EchoLog/api/v1"
	"EchoLog/internal/auth"
	"context"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type subjectContextKey struct {
}

func getSubjectFromContext(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

var authenticateMiddlewareFunc grpc_auth.AuthFunc = func(ctx context.Context) (context.Context, error) {
	p, ok := peer.FromContext(ctx)

	if !ok {
		return nil, status.New(codes.Unknown, "peer not found").Err()
	}

	if p.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}
	tlsInfo := p.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)
	return ctx, nil
}

type CommitLog interface {
	Append(*api.LogRecord) (uint64, error)
	Read(uint64) (*api.LogRecord, error)
}

type Config struct {
	CommitLog  CommitLog
	Authorizer *auth.Authorizer
}

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

var _ api.LogServer = (*grpcServer)(nil)

func newGrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

func NewGrpcServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {

	opts = append(opts, grpc.StreamInterceptor(
		grpc_middleware.ChainStreamServer(
			grpc_auth.StreamServerInterceptor(authenticateMiddlewareFunc),
		)), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
		grpc_auth.UnaryServerInterceptor(authenticateMiddlewareFunc),
	)))

	gsrv := grpc.NewServer(opts...)
	srv, err := newGrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	if err := s.Authorizer.Authorize(
		getSubjectFromContext(ctx),
		objectWildcard,
		produceAction,
	); err != nil {
		return nil, err
	}
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (
	*api.ConsumeResponse, error) {
	if err := s.Authorizer.Authorize(
		getSubjectFromContext(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

func (s *grpcServer) ProduceStream(
	stream api.Log_ProduceStreamServer,
) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}
func (s *grpcServer) ConsumeStream(
	req *api.ConsumeRequest,
	stream api.Log_ConsumeStreamServer,
) error {

	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}
