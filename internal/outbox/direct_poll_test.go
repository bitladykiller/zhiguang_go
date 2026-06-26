package outbox

import (
	"context"
	"testing"
)

func TestDirectPollConsumer_ID(t *testing.T) {
	var d *DirectPollConsumer
	_ = d
}

func TestDirectPollConsumer_Start_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var c *DirectPollConsumer
	c.Start(ctx)
}
