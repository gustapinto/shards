/*
Package shards is a client-side database sharding wrapper around [database/sql].

It works internally handling the hassle of managing the [database/sql.DB] connections for
each shard.

Shards also provide some utility functions and data structures, such as a type thread-safe
map (see [github.com/gustapinto/shards.SafeMap])
*/
package shards

import (
	"database/sql"
	"errors"
)

var (
	_shards *SafeMap[string, *Shard] = nil

	// ErrShardNotFound Is used when a shard is not found on the lookup table for
	// the provided key
	ErrShardNotFound = errors.New("shard not found")
)

// DoFunc Is the function type used by [github.com/gustapinto/shards.On] and [github.com/gustapinto/shards.OnAll], it
// must return [true] for the transaction to commit
type DoFunc func(shard Shard, tx *sql.Tx) (commit bool, err error)

type Shard struct {
	// The shards unique key
	Key string

	// The shard database connection string
	DSN string

	// The database driver
	Driver string

	// The shard underlying database connection, used only internally by shards lib
	_db *sql.DB
}

// NewShard Initialize a new [github.com/gustapinto/shards.Shard] object without
// connecting to the underlying [database/sql.DB] connection
func NewShard(key, driver, dsn string) Shard {
	return Shard{
		Key:    key,
		DSN:    dsn,
		Driver: driver,
		_db:    nil,
	}
}

// Clone Returns a deep copy of the shard, without the private fields
func (s Shard) Clone() Shard {
	return Shard{
		Key:    s.Key,
		DSN:    s.DSN,
		Driver: s.Driver,
	}
}

// Register Adds new shards to the internal shard lookup table, note that the key must be unique,
// otherwise the existing shard will de overwritten and its underlying [database/sql.DB]
// connection will be closed
func Register(shards ...Shard) error {
	for _, shard := range shards {
		if err := registerShard(shard); err != nil {
			return err
		}
	}

	return nil
}

// CloseAll Closes all shards connections and returns a slice of eventual errors
func CloseAll() (errs []error) {
	for _, key := range _shards.Keys() {
		if err := closeShard(key); err != nil {
			errs = append(errs, err)
		}
	}

	return errs
}

// DB Provides a lower level abstraction that returns the underlying database
// connection for a registered shard, or [nil] if the shard does not exist on registry,
// it is intended to be used by people migrating from [database/sql] to [github.com/gustapinto/shards],
// if you are writing your code fromm scratch using shards than have a look at [github.com/gustapinto/shards.On]
func DB(key string) *sql.DB {
	if _shards == nil {
		return nil
	}

	shard, exists := _shards.Get(key)
	if !exists {
		return nil
	}

	if shard._db == nil {
		db, err := connectToDatabase(*shard)
		if err != nil {
			return nil
		}

		shard._db = db
	}

	return shard._db
}

// On Provides a higher level abstraction over [github.com/gustapinto/shards.DB]
// providing a easier whey to do work on the shard, please avoid calling Rollback
// and Commit from within the "do" function. For the transaction to commit it is
// necessary to return true from the [github.com/gustapinto/shards.DoFunc]
func On(key string, do DoFunc) error {
	shard, exists := _shards.Get(key)
	if !exists {
		return ErrShardNotFound
	}

	db := DB(key)
	if db == nil {
		return ErrShardNotFound
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

// OnAll Runs the supplied function with all registered shards, stopping on the first non-nil error.
// This has the same behavior as calling [github.com/gustapinto/shards.On] on all the registered shards.
// The transaction isolation level is shard-specific. For the transaction to commit it is necessary to
// return true from the [github.com/gustapinto/shards.DoFunc]
func OnAll(do DoFunc) error {
	for _, key := range _shards.Keys() {
		if err := On(key, do); err != nil {
			return err
		}
	}

	return nil
}

// Lookup Returns an array of all registered shards. Note that this function does not
// guarantee order of the returned shards slice
func Lookup() (shards []Shard) {
	if _shards == nil || _shards.Len() == 0 {
		return nil
	}

	for _, shard := range _shards.Values() {
		shards = append(shards, *shard)
	}

	return shards
}

func registerShard(shard Shard) error {
	if _shards == nil {
		_shards = NewSafeMap[string, *Shard]()
	} else {
		oldShard, exists := _shards.Get(shard.Key)
		if exists {
			if err := oldShard._db.Close(); err != nil {
				return err
			}
		}
	}

	db, err := connectToDatabase(shard)
	if err != nil {
		return err
	}

	_shards.Set(shard.Key, &Shard{
		Key:    shard.Key,
		DSN:    shard.DSN,
		Driver: shard.Driver,
		_db:    db,
	})

	return nil
}

func connectToDatabase(shard Shard) (*sql.DB, error) {
	db, err := sql.Open(shard.Driver, shard.DSN)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func isErrTxDone(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, sql.ErrTxDone)
}

func closeShard(key string) error {
	if _shards == nil {
		return nil
	}

	shard, exists := _shards.Get(key)
	if !exists {
		return nil
	}

	if err := shard._db.Close(); err != nil {
		return err
	}

	_shards.Del(key)

	return nil
}
