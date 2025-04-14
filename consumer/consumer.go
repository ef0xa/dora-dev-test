package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"dora-dev-test/data"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Start the consumer. This blocks until the context is canceled; run it in a goroutine.
func Start(ctx context.Context,
	kafka *kgo.Client,
	saveTick func(ctx context.Context, tick data.Tick) error,
	topics ...string,
) error {
	kafka.AddConsumeTopics(topics...)
	var tick []data.Tick
	for {
		tick, err := poll(ctx, kafka, tick[:0])
		if err != nil {
			return err
		}
		for _, t := range tick {
			if err := saveTick(ctx, t); err != nil {
				return err
			}
		}
	}
}

func poll(ctx context.Context, kafka *kgo.Client, buf []data.Tick) ([]data.Tick, error) {
	slog.Debug("polling records")
	fetches := kafka.PollRecords(ctx, 80)
	var err error
	for _, e := range fetches.Errors() {
		err = errors.Join(err, fmt.Errorf("partition %d: topic %s: %w", e.Partition, e.Topic, e.Err))
	}
	if err != nil {
		slog.Error("got an error", "err", err.Error())
		return nil, err
	}

	for iter := fetches.RecordIter(); !iter.Done(); {
		rec := iter.Next()
		var tick data.Tick
		if err := json.Unmarshal(rec.Value, &tick); err != nil {
			return nil, err
		}
		buf = append(buf, tick)
	}
	return buf, nil
}
