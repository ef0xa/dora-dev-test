package redisds

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"dora-dev-test/data"

	"github.com/redis/go-redis/v9"
)

type DataStore struct {
	c *redis.Client
}

func (d DataStore) SaveTick(ctx context.Context, tick data.Tick) error {
	rollback := func(err error) { panic(err) }
	for _, v := range []struct {
		key   string
		value float64
	}{
		{tick.AssetID + "last_price", tick.LastPrice},
		{tick.AssetID + "last_size", tick.LastSize},
		{tick.AssetID + "best_bid", tick.BestBid},
		{tick.AssetID + "best_ask", tick.BestAsk},
	} {
		if err := d.c.TSAdd(ctx, v.key, tick.Timestamp.Unix(), v.value).Err(); err != nil {
			rollback(err)
		}
	}
	slog.Debug("savetick: ok", "tick", tick)
	return nil
}

func ptrTo[T any](t T) *T {
	return &t
}

func (d DataStore) GetTicks(ctx context.Context, assetID string, from, to *int64, limit int) ([]data.Tick, error) {
	if from == nil {
		from = ptrTo[int64](math.MinInt64)
	}
	if to == nil {
		to = ptrTo[int64](math.MaxInt64)
	}
	f, t := int(*from), int(*to)

	priceKey, sizeKey, bidKey, askKey := assetID+"last_price", assetID+"last_size", assetID+"best_bid", assetID+"best_ask"
	lastPrices, err := d.c.TSRange(ctx, priceKey, f, t).Result()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", priceKey, err)
	}
	sizes, err := d.c.TSRange(ctx, sizeKey, f, t).Result()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", sizeKey, err)
	}

	bids, err := d.c.TSRange(ctx, bidKey, f, t).Result()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", bidKey, err)
	}
	asks, err := d.c.TSRange(ctx, askKey, f, t).Result()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", askKey, err)
	}

	if len(lastPrices) != len(sizes) || len(bids) != len(asks) || len(bids) != len(lastPrices) {
		return nil, fmt.Errorf("expected all sizes for tick timeseries to match, but got lastPrices =%d, sizes = %d, bids = %d ,asks = %d", len(lastPrices), len(sizes), len(bids), len(asks))
	}

	ticks := make([]data.Tick, len(lastPrices))
	for i := range ticks {
		ticks[i] = data.Tick{
			AssetID:   assetID,
			Timestamp: time.Unix(asks[i].Timestamp, 0).UTC(),
			LastPrice: lastPrices[i].Value,
			LastSize:  sizes[i].Value,
			BestBid:   bids[i].Value,
			BestAsk:   asks[i].Value,
		}
	}
	return ticks, nil
}

func NewDataStore(c *redis.Client) DataStore {
	return DataStore{c}
}
