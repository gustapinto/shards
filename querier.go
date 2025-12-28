package shards

import (
	"database/sql"
	"errors"
)

var (
	// ErrNoLoadedShards Is used when there is no loaded shards into the [github.com/gustapinto/shards.Querier]
	ErrNoLoadedShards = errors.New("no shards loaded into querier")

	// ErrFailedToOpenDB Is used when the querier cannot open a connection to the database shard
	ErrFailedToOpenDB = errors.New("failed to open a database connection for shard")
)

// DoFunc Is the function type used by [Querier.Do], it must return (true, nil) for the transaction to commit
//
// Please avoid commiting/rolling back the transaction inside the [DoFunc], as it may cause conflicts
// and errors
type DoFunc func(shard Shard, tx *sql.Tx) (commit bool, err error)

// Querier Provides an easy to use higher level transactional API over [github.com/gustapinto/shards.DB].
//
// Note that to use the Querier API one also needs to register the shards using [github.com/gustapinto/shards.Register]
type Querier struct {
	loadedShards []*Shard

	failFast bool
}

// On Select and load the shards by key into the querier, the selected shards will be used on
// the [Querier.Do] method. If a shard key does not exists in the registry it will be ignored
func On(keys ...string) *Querier {
	var shards []*Shard
	for _, key := range keys {
		if shard, exists := shardsLookupTable.Get(key); exists {
			shards = append(shards, shard)
		}
	}

	return &Querier{
		loadedShards: shards,
	}
}

// OnAll Select and load all registered shards into the querier, the selected shards will be used on
// the [Querier.Do] method
func OnAll() *Querier {
	return On(shardsLookupTable.Keys()...)
}

// WithFailFast Stops any loop operation on the first non-nil error inside the Querier
func (sq *Querier) WithFailFast() *Querier {
	sq.failFast = true

	return sq
}

// Do Execute the [github.com/gustapinto/shards.DoFunc] against the [github.com/gustapinto/shards.ShardQuerier]
// selected shards with a shard-specific isolation level.
func (sq *Querier) Do(do DoFunc) (errs []error) {
	if len(sq.loadedShards) == 0 {
		return nil
	}

	for _, shard := range sq.loadedShards {
		if err := doInShard(shard, do); err != nil {
			errs = append(errs, err)

			if sq.failFast {
				return errs
			}

			return
		}
	}

	return nil
}

// Package level private functions
func doInShard(shard *Shard, do DoFunc) error {
	db := DB(shard.Key)
	if db == nil {
		return ErrFailedToOpenDB
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	commit, err := do(shard.Clone(), tx)
	if err != nil {
		if rollErr := tx.Rollback(); !isErrTxDone(rollErr) {
			return err
		}

		return err
	}

	if commit {
		if err := tx.Commit(); !isErrTxDone(err) {
			return err
		}
	}

	return nil
}

func isErrTxDone(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, sql.ErrTxDone)
}
