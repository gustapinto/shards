/*
Shards is a client-side database sharding wrapper around [database/sql].

It works internally handling the hassle of managing the [database/sql.DB] connections for
each shard.

Shards also provide some utility functions and data structures, such as a type thread-safe
map (see [github.com/gustapinto/shards.SafeMap])
*/
package shards

import (
	"database/sql"
)

type Shard struct {
	// The shards unique key
	Key string

	// The shard database connection string
	DSN string

	// The database driver
	Driver string

	// The shard underlaying database connection, used only iternally by shards lib
	_db *sql.DB
}

// NewShard Initialize a new [Shard] object
func NewShard(key, driver, dsn string) Shard {
	return Shard{
		Key:    key,
		DSN:    dsn,
		Driver: driver,
		_db:    nil,
	}
}

var _shards *SafeMap[string, *Shard]

// Register Adds a new shard to the shard registry, not that the key must be unique,
// otherwrise the existing shard will de overwritten and its underlaying [database/sql.DB]
// connection will be closed
func Register(shards ...Shard) error {
	for _, shard := range shards {
		if err := _registerShard(shard); err != nil {
			return err
		}
	}

	return nil
}

// Close Closes a shard connection to database and removes it from the registry
func Close(key string) error {
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

// CloseAll Closes all shards connections
func CloseAll() (errs []error) {
	for _, key := range _shards.Keys() {
		if err := Close(key); err != nil {
			errs = append(errs, err)
		}
	}

	return errs
}

// On Returns the underlaying database connection for a registered shard, or [nil]
// if tge shard does not exists on registry
func On(key string) *sql.DB {
	if _shards == nil {
		return nil
	}

	shard, exists := _shards.Get(key)
	if !exists {
		return nil
	}

	if shard._db == nil {
		db, err := _connectToDatabase(*shard)
		if err != nil {
			return nil
		}

		shard._db = db
	}

	return shard._db
}

// Each Runs the supplied function with all registered shards, stopping on
// the first non-nil error. Note that [github.com/gustapinto/shards.Each] does not guarantee order of execution
func Each(fn func(shard *Shard) error) error {
	for _, shard := range _shards.Lookup() {
		if err := fn(shard); err != nil {
			return err
		}
	}

	return nil
}

// Private functions
func _registerShard(shard Shard) error {
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

	db, err := _connectToDatabase(shard)
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

func _connectToDatabase(shard Shard) (*sql.DB, error) {
	db, err := sql.Open(shard.Driver, shard.DSN)
	if err != nil {
		return nil, err
	}

	return db, nil
}
