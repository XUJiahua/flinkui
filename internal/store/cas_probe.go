package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

// ProbeCAS checks conditional create, concurrent conditional update, and
// stale ETag rejection on a disposable object under the coordination prefix.
func (c *Coord) ProbeCAS(ctx context.Context) (err error) {
	key := "cas-probes/" + uuid.NewString()
	initial := &HandoffRecord{Group: key, Epoch: 1, Phase: PhaseStable}
	if err := c.WriteHandoffCAS(ctx, key, initial, ""); err != nil {
		return fmt.Errorf("probe conditional create: %w", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_, deleteErr := c.client.DeleteObject(cleanup, &s3.DeleteObjectInput{
			Bucket: aws.String(c.bucket), Key: aws.String(c.key(key)),
		})
		if deleteErr != nil {
			err = errors.Join(err, fmt.Errorf("clean up CAS probe %q: %w", c.key(key), deleteErr))
		}
	}()

	if err := expectProbeConflict("duplicate create", c.WriteHandoffCAS(ctx, key, &HandoffRecord{Epoch: 99}, "")); err != nil {
		return err
	}
	rec, etag, exists, err := c.ReadHandoffWithETag(ctx, key)
	if err != nil {
		return err
	}
	if !exists || rec.Group != key || rec.Epoch != 1 {
		return fmt.Errorf("CAS probe: conditional create was not preserved")
	}

	type result struct {
		owner string
		err   error
	}
	const contenders = 8
	results := make(chan result, contenders)
	for i := range contenders {
		owner := fmt.Sprintf("candidate-%d", i)
		go func() {
			err := c.WriteHandoffCAS(ctx, key, &HandoffRecord{
				Group: key, Epoch: 2, Phase: PhaseStable, ActiveClusterID: owner,
			}, etag)
			results <- result{owner: owner, err: err}
		}()
	}
	winners := 0
	var winner string
	var unexpected error
	for range contenders {
		r := <-results
		switch {
		case r.err == nil:
			winners++
			winner = r.owner
		case !errors.Is(r.err, ErrHandoffConflict):
			unexpected = errors.Join(unexpected, r.err)
		}
	}
	if winners != 1 || unexpected != nil {
		return errors.Join(fmt.Errorf("CAS probe: concurrent update got %d winners, want 1", winners), unexpected)
	}
	if err := expectProbeConflict("stale update", c.WriteHandoffCAS(ctx, key, &HandoffRecord{Epoch: 99}, etag)); err != nil {
		return err
	}
	rec, _, exists, err = c.ReadHandoffWithETag(ctx, key)
	if err != nil {
		return err
	}
	if !exists || rec.Group != key || rec.Epoch != 2 || rec.ActiveClusterID != winner {
		return fmt.Errorf("CAS probe: concurrent winner was not preserved")
	}
	return nil
}

func expectProbeConflict(operation string, err error) error {
	if errors.Is(err, ErrHandoffConflict) {
		return nil
	}
	if err == nil {
		return fmt.Errorf("CAS probe: storage accepted %s", operation)
	}
	return fmt.Errorf("CAS probe: %s did not return a precondition failure: %w", operation, err)
}
