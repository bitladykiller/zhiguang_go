package idgen

import (
	"testing"

	"github.com/zhiguang/app/pkg/config"
)

func TestNewSnowflakeGenerator_Success(t *testing.T) {
	cfg := &config.IDGeneratorConfig{MachineID: 1, WorkerID: 1}
	gen, err := NewSnowflakeGenerator(cfg)
	if err != nil {
		t.Fatalf("NewSnowflakeGenerator: %v", err)
	}
	if gen == nil {
		t.Fatal("expected non-nil generator")
	}
	if gen.MachineID() != 1 {
		t.Errorf("MachineID = %d, want 1", gen.MachineID())
	}
	if gen.WorkerID() != 1 {
		t.Errorf("WorkerID = %d, want 1", gen.WorkerID())
	}
}

func TestNewSnowflakeGenerator_NilConfig(t *testing.T) {
	_, err := NewSnowflakeGenerator(nil)
	if err == nil {
		t.Fatal("expected error for nil config")
	}
}

func TestNewSnowflakeGenerator_InvalidMachineID(t *testing.T) {
	cfg := &config.IDGeneratorConfig{MachineID: -1, WorkerID: 0}
	_, err := NewSnowflakeGenerator(cfg)
	if err == nil {
		t.Fatal("expected error for negative machine_id")
	}
}

func TestNewSnowflakeGenerator_TooLargeMachineID(t *testing.T) {
	cfg := &config.IDGeneratorConfig{MachineID: 32, WorkerID: 0}
	_, err := NewSnowflakeGenerator(cfg)
	if err == nil {
		t.Fatal("expected error for machine_id > 31")
	}
}

func TestNewSnowflakeGenerator_InvalidWorkerID(t *testing.T) {
	cfg := &config.IDGeneratorConfig{MachineID: 0, WorkerID: -1}
	_, err := NewSnowflakeGenerator(cfg)
	if err == nil {
		t.Fatal("expected error for negative worker_id")
	}
}

func TestNewSnowflakeGenerator_TooLargeWorkerID(t *testing.T) {
	cfg := &config.IDGeneratorConfig{MachineID: 0, WorkerID: 32}
	_, err := NewSnowflakeGenerator(cfg)
	if err == nil {
		t.Fatal("expected error for worker_id > 31")
	}
}

func TestNextID_Unique(t *testing.T) {
	cfg := &config.IDGeneratorConfig{MachineID: 0, WorkerID: 0}
	gen, err := NewSnowflakeGenerator(cfg)
	if err != nil {
		t.Fatalf("NewSnowflakeGenerator: %v", err)
	}

	ids := make(map[uint64]bool)
	for i := 0; i < 100; i++ {
		id := gen.NextID()
		if ids[id] {
			t.Fatalf("duplicate ID: %d", id)
		}
		ids[id] = true
	}
}

func TestNextID_Monotonic(t *testing.T) {
	cfg := &config.IDGeneratorConfig{MachineID: 2, WorkerID: 3}
	gen, err := NewSnowflakeGenerator(cfg)
	if err != nil {
		t.Fatalf("NewSnowflakeGenerator: %v", err)
	}

	var prev uint64
	for i := 0; i < 50; i++ {
		id := gen.NextID()
		if i > 0 && id <= prev {
			t.Fatalf("ID %d is not greater than previous %d", id, prev)
		}
		prev = id
	}
}

func TestNextID_ConcurrentSafe(t *testing.T) {
	cfg := &config.IDGeneratorConfig{MachineID: 1, WorkerID: 2}
	gen, err := NewSnowflakeGenerator(cfg)
	if err != nil {
		t.Fatalf("NewSnowflakeGenerator: %v", err)
	}

	done := make(chan struct{})
	ids := make(chan uint64, 200)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 20; j++ {
				ids <- gen.NextID()
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
	close(ids)

	set := make(map[uint64]bool)
	for id := range ids {
		if set[id] {
			t.Fatalf("duplicate ID in concurrent access: %d", id)
		}
		set[id] = true
	}
}

func TestNodeID(t *testing.T) {
	cfg := &config.IDGeneratorConfig{MachineID: 3, WorkerID: 7}
	gen, _ := NewSnowflakeGenerator(cfg)

	nodeID := gen.NodeID()
	want := int64((3 << 5) | 7)
	if nodeID != want {
		t.Errorf("NodeID = %d, want %d", nodeID, want)
	}
}

func TestNodeID_NilReceiver(t *testing.T) {
	var gen *SnowflakeGenerator
	if id := gen.NodeID(); id != -1 {
		t.Errorf("NodeID() on nil = %d, want -1", id)
	}
	if id := gen.MachineID(); id != -1 {
		t.Errorf("MachineID() on nil = %d, want -1", id)
	}
	if id := gen.WorkerID(); id != -1 {
		t.Errorf("WorkerID() on nil = %d, want -1", id)
	}
}

func TestNewSnowflakeGenerator_MaxBoundary(t *testing.T) {
	cfg := &config.IDGeneratorConfig{MachineID: 31, WorkerID: 31}
	gen, err := NewSnowflakeGenerator(cfg)
	if err != nil {
		t.Fatalf("NewSnowflakeGenerator at boundary: %v", err)
	}
	if gen.NodeID() != int64((31<<5)|31) {
		t.Errorf("NodeID = %d, want %d", gen.NodeID(), (31<<5)|31)
	}
}