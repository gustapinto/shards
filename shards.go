/*
Package shards is a client-side database sharding wrapper around [database/sql].

It works by internally handling the hassle of managing the [database/sql.DB] connections for
each shard ([github.com/gustapinto/shards.DB]) and by providing some higher level APIs over
these ([github.com/gustapinto/shards.Querier]).

Shards also provide some utility functions and data structures, such as a type thread-safe
map (see [github.com/gustapinto/shards.SafeMap])
*/
package shards

import (
	"database/sql"
	"errors"
)

var (
	shardsLookupTable *SafeMap[string, *Shard] = NewSafeMap[string, *Shard]()

	// ErrEmptyRegistry Is used when there is a attempt to access a non initialized
	// registry
	ErrEmptyRegistry = errors.New("empty registry")
)

type Shard struct {
	// The shards unique key
	Key string

	// The shard database connection string
	DSN string

	// The database driver
	Driver string

	// The shard underlying database connection, used only internally by shards lib
	db *sql.DB
}

// NewShard Initialize a new [github.com/gustapinto/shards.Shard] object without
// connecting to the underlying [database/sql.DB] connection
func NewShard(key, driver, dsn string) Shard {
	return Shard{
		Key:    key,
		DSN:    dsn,
		Driver: driver,
		db:     nil,
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
//
// The shard database connection will be lazily opened when first calling the
// [github.com/gustapinto/shards.DB] function for the shard
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
	if shardsLookupTable == nil {
		return nil
	}

	for _, key := range shardsLookupTable.Keys() {
		if err := closeShard(key); err != nil {
			errs = append(errs, err)
		}
	}

	return errs
}

// DB Provides a lower level abstraction that returns the underlying database
// connection for a registered shard, or [nil] if the shard does not exist on registry,
// it is intended to be used by people migrating from [database/sql] to [github.com/gustapinto/shards].
//
// If you are writing your code from scratch using shards than have a look at [github.com/gustapinto/shards.Querier].
//
// Note that there is no need to defer Close this [database/sql.DB] connection, as it will already
// be closed by the [github.com/gustapinto/shards.CloseAll] method
func DB(key string) *sql.DB {
	if shardsLookupTable == nil {
		return nil
	}

	shard, exists := shardsLookupTable.Get(key)
	if !exists {
		return nil
	}

	if shard.db == nil {
		db, err := sql.Open(shard.Driver, shard.DSN)
		if err != nil {
			return nil
		}

		shard.db = db
	}

	return shard.db
}

// Lookup Returns an array of all registered shards. Note that this function does not
// guarantee order of the returned shards slice
func Lookup() (shards []Shard) {
	if shardsLookupTable == nil || shardsLookupTable.Len() == 0 {
		return nil
	}

	for _, shard := range shardsLookupTable.Values() {
		shards = append(shards, *shard)
	}

	return shards
}

// Package level private functions
func registerShard(shard Shard) error {
	if shardsLookupTable == nil {
		shardsLookupTable = NewSafeMap[string, *Shard]()
	} else {
		oldShard, exists := shardsLookupTable.Get(shard.Key)
		if exists && oldShard.db != nil {
			if err := oldShard.db.Close(); err != nil {
				return err
			}
		}
	}

	shardsLookupTable.Set(shard.Key, &Shard{
		Key:    shard.Key,
		DSN:    shard.DSN,
		Driver: shard.Driver,
		db:     nil,
	})

	return nil
}

func closeShard(key string) error {
	if shardsLookupTable == nil {
		return nil
	}

	shard, exists := shardsLookupTable.Get(key)
	if !exists {
		return nil
	}

	if err := shard.db.Close(); err != nil {
		return err
	}

	shardsLookupTable.Del(key)

	return nil
}
