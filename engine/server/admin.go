package server

import (
	"context"
	"errors"
	"net/http"

	"connectrpc.com/connect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
)

// adminService implements syncplanev1connect.AdminServiceHandler on top of a
// Manager. It is the trusted control plane: every Register call passes the
// Manager's RegisterAuthorizer before touching state.
type adminService struct {
	syncplanev1connect.UnimplementedAdminServiceHandler
	mgr *Manager
}

// newAdminHandler builds the AdminService mount path and handler.
func newAdminHandler(mgr *Manager) (string, http.Handler) {
	return syncplanev1connect.NewAdminServiceHandler(&adminService{mgr: mgr})
}

func (a *adminService) Register(
	ctx context.Context,
	req *connect.Request[syncplanev1.RegisterRequest],
) (*connect.Response[syncplanev1.RegisterResponse], error) {
	appID := req.Msg.GetAppId()
	if appID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("app_id is required"))
	}
	// Trust gate first: deny-by-default, never touch state for an unauthorized
	// caller.
	if err := a.mgr.authorizer.CanRegister(ctx, RegisterInfo{AppID: appID, Header: req.Header()}); err != nil {
		return nil, connect.NewError(connect.CodePermissionDenied, err)
	}

	// Optimistic concurrency: reject if the app moved out from under the caller.
	if expected := req.Msg.GetExpectedRevision(); expected != "" {
		current := ""
		if app := a.mgr.lookup(appID); app != nil {
			current = app.srv.SchemaRevision()
		}
		if current != expected {
			return nil, connect.NewError(connect.CodeFailedPrecondition,
				errors.New("expected_revision does not match the app's current revision"))
		}
	}

	res, err := a.mgr.Register(ctx, appID, req.Msg.GetDescriptorSet())
	if err != nil {
		// A conflicting (non-additive) schema is a precondition failure; a
		// malformed descriptor set is invalid argument.
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	return connect.NewResponse(&syncplanev1.RegisterResponse{
		Revision:    res.Revision,
		Status:      wireStatus(res.Status),
		EntityNames: res.Entities,
	}), nil
}

func (a *adminService) GetSchema(
	ctx context.Context,
	req *connect.Request[syncplanev1.GetSchemaRequest],
) (*connect.Response[syncplanev1.GetSchemaResponse], error) {
	appID := req.Msg.GetAppId()
	if appID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("app_id is required"))
	}
	schemas, err := a.mgr.root.ListAppSchemas(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	for _, sc := range schemas {
		if sc.AppID == appID {
			return connect.NewResponse(&syncplanev1.GetSchemaResponse{
				DescriptorSet: sc.DescriptorSet,
				Revision:      sc.Revision,
				Found:         true,
			}), nil
		}
	}
	return connect.NewResponse(&syncplanev1.GetSchemaResponse{Found: false}), nil
}

func wireStatus(s RegisterStatus) syncplanev1.RegisterStatus {
	switch s {
	case RegisterCreated:
		return syncplanev1.RegisterStatus_REGISTER_STATUS_CREATED
	case RegisterUpdated:
		return syncplanev1.RegisterStatus_REGISTER_STATUS_UPDATED
	default:
		return syncplanev1.RegisterStatus_REGISTER_STATUS_UNCHANGED
	}
}
