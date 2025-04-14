package service

import (
	"context"
	"time"

	api "dora-dev-test/api/v1"
	"dora-dev-test/data"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Service struct {
	api.UnimplementedDoraDevTestServiceServer
	getTicks func(ctx context.Context, assetID string, from, to *int64, limit int) ([]data.Tick, error)
}

// you'd use proto

func (s Service) HealthCheck(ctx context.Context, empty *emptypb.Empty) (*api.HealthCheckResponse, error) {
	return &api.HealthCheckResponse{LastHeartbeat: timestamppb.New(time.Now())}, nil
}

func (s Service) GetTicks(ctx context.Context, request *api.GetTicksRequest) (*api.GetTicksResponse, error) {
	var from, to *int64
	if secs := request.GetStart().GetSeconds(); secs != 0 {
		from = &secs
	}
	if secs := request.GetEnd().GetSeconds(); secs != 0 {
		to = &secs
	}

	ticks, err := s.getTicks(ctx, request.GetSymbol(), from, to, int(request.GetLimit()))
	if err != nil {
		return nil, err
	}
	apiTicks := make([]*api.Tick, len(ticks))
	for i := range ticks {
		apiTicks[i] = &api.Tick{
			Timestamp: &timestamppb.Timestamp{Seconds: int64(ticks[i].Timestamp.Unix())},
			LastPrice: ticks[i].LastPrice,
			LastSize:  ticks[i].LastSize,
			BestBid:   ticks[i].BestBid,
		}
	}
	return &api.GetTicksResponse{Ticks: apiTicks}, nil
}

func NewService(getTicks func(ctx context.Context, assetID string, from, to *int64, limit int) ([]data.Tick, error)) Service {
	return Service{getTicks: getTicks}
}
