package server

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"connectrpc.com/connect"
)

// reqLog returns a logger scoped to one RPC invocation. The rpc and workspace
// keys are always present; client is added only when non-empty (Poke and
// anonymous reads carry none). In-handler logging uses this for semantic
// events (resets, rejections, adoptions); RPC-level latency/code logging is
// the loggingInterceptor's job.
//
// ctx supplies the trace and span ids when a trace is recording, so a semantic
// log line can be pivoted to the trace that produced it.
func (s *Server) reqLog(ctx context.Context, rpc, workspace, client string) *slog.Logger {
	l := s.log().With("rpc", rpc, "workspace", workspace)
	if client != "" {
		l = l.With("client", client)
	}
	if args := traceIDArgs(ctx); len(args) > 0 {
		l = l.With(args...)
	}
	return l
}

// loggingInterceptor logs every RPC's procedure, outcome code and latency
// once, uniformly, so the handlers themselves carry only semantic logging. A
// successful call logs at Debug; a failure logs at the level its Connect code
// warrants (client-caused codes at Info, infrastructure codes at Error), so a
// log scraped at Info surfaces server faults without the hot-path noise.
func loggingInterceptor(logger *slog.Logger) connect.Interceptor {
	return &logInterceptor{logger: logger}
}

type logInterceptor struct {
	logger *slog.Logger
}

func (i *logInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		start := time.Now()
		resp, err := next(ctx, req)
		i.log(ctx, "unary RPC handled", procedure(req.Spec().Procedure), start, err)
		return resp, err
	}
}

// WrapStreamingClient is a no-op: the engine only serves streams, never calls
// them, so there is no client side to instrument.
func (i *logInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (i *logInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		proc := procedure(conn.Spec().Procedure)
		start := time.Now()
		i.logger.LogAttrs(ctx, slog.LevelDebug, "stream opened", slog.String("rpc", proc))
		err := next(ctx, conn)
		i.log(ctx, "stream closed", proc, start, err)
		return err
	}
}

func (i *logInterceptor) log(ctx context.Context, msg, proc string, start time.Time, err error) {
	level := slog.LevelDebug
	attrs := []slog.Attr{
		slog.String("rpc", proc),
		slog.Duration("elapsed", time.Since(start)),
	}
	attrs = append(attrs, traceIDAttrs(ctx)...)
	if err != nil {
		code := connect.CodeOf(err)
		level = levelForCode(code)
		attrs = append(attrs, slog.String("code", code.String()), slog.Any("err", err))
	}
	i.logger.LogAttrs(ctx, level, msg, attrs...)
}

// levelForCode maps a Connect error code to a log level: codes a client can
// provoke (bad arguments, conflicts, exhausted limits) are routine and log at
// Info; cancellation/deadline are the client walking away and stay at Debug;
// everything else is an infrastructure fault and logs at Error.
func levelForCode(code connect.Code) slog.Level {
	switch code {
	case connect.CodeCanceled, connect.CodeDeadlineExceeded:
		return slog.LevelDebug
	case connect.CodeInvalidArgument, connect.CodeNotFound, connect.CodeAlreadyExists,
		connect.CodePermissionDenied, connect.CodeUnauthenticated, connect.CodeFailedPrecondition,
		connect.CodeResourceExhausted, connect.CodeAborted, connect.CodeOutOfRange:
		return slog.LevelInfo
	default:
		return slog.LevelError
	}
}

// procedure trims the leading "/package.Service/" from a Connect procedure
// path, leaving just the method name (e.g. "Pull") for compact log lines.
func procedure(full string) string {
	if i := strings.LastIndexByte(full, '/'); i >= 0 {
		return full[i+1:]
	}
	return full
}
