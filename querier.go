package shards

import (
	"database/sql"
	"errors"
	"fmt"
)

var (
	// ErrFailedToOpenDB Is used when the querier cannot open a connection to the database shard
	ErrFailedToOpenDB = errors.New("failed to open a database connection for shard")
)

// DoFunc Is the function type used by [Querier.Do]
type DoFunc func(shard *Shard, db *sql.DB) error

// Querier Provides an easy to use higher level transactional API over [github.com/gustapinto/shards.DB].
//
// It should not be treated as a long-lived object, instead treat it as a transactional one, you get a new one
// with [github.com/gustapinto/shards.On] or [github.com/gustapinto/shards.OnAll], do what needs to be done
// and then let the GC do its job.
//
// Note that to use the Querier API one also needs to register the shards using [github.com/gustapinto/shards.Register]
type Querier struct {
	failFast       bool
	selectedShards []*Shard
}

// On Select and load the shards by key into the querier, the selected shards will be used on
// the [Querier.Do] method. If a shard key does not exists in the registry it will be ignored
func On(keys ...string) *Querier {
	selectedShards := make([]*Shard, 0, len(keys))

	for _, key := range keys {
		if shard, exists := shardsLookupTable.Get(key); exists {
			selectedShards = append(selectedShards, shard)
		}
	}

	return &Querier{
		selectedShards: selectedShards,
	}
}

// OnAll Select and load all registered shards into the querier, the selected shards will be used on
// the [Querier.Do] method
func OnAll() *Querier {
	return &Querier{
		selectedShards: shardsLookupTable.Values(),
	}
}

// FailFast Configuration stops the [Querier.Do] shard loop operation on the first non-nil error
func (sq *Querier) FailFast() *Querier {
	sq.failFast = true

	return sq
}

// Do Execute the [github.com/gustapinto/shards.DoFunc] against the [github.com/gustapinto/shards.ShardQuerier]
// selected shards with a shard-specific isolation level.
//
// Use it with the [Querier.FailFast] configuration method in order to stop the execution on the
// first selected shard with a non-nil error, otherwise it will return a [errors.Join] of all errors
func (sq *Querier) Do(do DoFunc) error {
	if len(sq.selectedShards) == 0 {
		return nil
	}

	return sq.doSequential(do)
}

// Package level private functions
func (sq *Querier) doSequential(do DoFunc) (err error) {
	for _, shard := range sq.selectedShards {
		if innerErr := doInShard(shard, do); innerErr != nil {
			if sq.failFast {
				return innerErr
			}

			err = errors.Join(err, errOnShard(shard, innerErr))
		}
	}

	return err
}

func doInShard(shard *Shard, do DoFunc) error {
	db := DB(shard.Key)
	if db == nil {
		return ErrFailedToOpenDB
	}

	if err := do(shard, db); err != nil {
		return err
	}

	return nil
}

func errOnShard(shard *Shard, err error) error {
	return fmt.Errorf("failed on shard %s with error %w", shard.Key, err)
}
