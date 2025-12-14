package shards

import (
	"database/sql"
	"errors"
	"fmt"
)

type ReplicationStrategy string

const (
	// ReplicateNone Disable shard data replication
	ReplicateNone ReplicationStrategy = "ReplicateNone"

	// ReplicateAll Replicates data to every shard replica synchronously
	// on every transaction, every replication operation must succeed for the transaction
	// to commit
	ReplicateAll ReplicationStrategy = "ReplicateAll"

	// ReplicateSome Replicates data to every shard replica synchronously
	// on every transaction, at least one replication operation must succeed
	// for the transaction to commit
	ReplicateSome ReplicationStrategy = "ReplicateSome" // TODO -> Unimplemented
)

// TODO -> Unused
type ReplicaMode string

const (
	// ReadWrite Enables the shard replica to be used on both Transact and Query calls
	ReadWrite ReplicaMode = "ReadWrite"

	// ReadOnly Enables the shard replica to be used only to read data on Query
	ReadOnly ReplicaMode = "ReadOnly"

	// WriteOnly Enables the shard replica to be used only to mutate data on Transact
	WriteOnly ReplicaMode = "WriteOnly"
)

type SqlShardReplica struct {
	// The replica unique key
	Key string

	// The replica database connection string
	DSN string

	// The replica database driver
	Driver string
}

func NewSqlShardReplica(key, driver, dsn string) SqlShardReplica {
	return SqlShardReplica{
		Key:    key,
		DSN:    dsn,
		Driver: driver,
	}
}

type SqlShard struct {
	// The shard unique key
	Key string

	// The shard database connection string
	DSN string

	// The database driver
	Driver string

	// The shard replicas
	//
	// Write behaviour: every Transact call to a shard will also be done
	// against its replicas using the ReplicationStrategy. To avoid a deep
	// replica stack only the shard-level replica slice will be registered in
	// the querier lookup map, so the replica key, as with the shard key must be
	// unique to avoid overwrites
	//
	// Read behaviour: the DB will first try to read from the shard itself, if its
	// db connection is failing then it will iter over all replicas and return
	// the the first non-connection-error result
	Replicas []SqlShardReplica
}

func NewSqlShard(key, driver, dsn string, replicas ...SqlShardReplica) SqlShard {
	return SqlShard{
		Key:      key,
		DSN:      dsn,
		Driver:   driver,
		Replicas: replicas,
	}
}

type sqlQuerierShardReplica struct {
	key string
	db  *sql.DB
}

type sqlQuerierShard struct {
	key      string
	db       *sql.DB
	replicas []sqlQuerierShardReplica
}

func newSqlQuerierShard(replicationStrategy ReplicationStrategy, shard *SqlShard) (*sqlQuerierShard, error) {
	db, err := sql.Open(shard.Driver, shard.DSN)
	if err != nil {
		return nil, err
	}

	entry := sqlQuerierShard{
		key:      shard.Key,
		db:       db,
		replicas: make([]sqlQuerierShardReplica, len(shard.Replicas)),
	}

	if replicationStrategy == ReplicateAll || replicationStrategy == ReplicateSome {
		for i, replica := range shard.Replicas {
			replicaDB, err := sql.Open(replica.Driver, replica.DSN)
			if err != nil {
				return nil, err
			}

			entry.replicas[i] = sqlQuerierShardReplica{
				key: replica.Key,
				db:  replicaDB,
			}
		}
	}

	return &entry, nil
}

type SqlQuerierConfig struct {
	ReplicationStrategy ReplicationStrategy
	Shards              []SqlShard
}

type SqlQuerier struct {
	replicationStrategy ReplicationStrategy
	shards              *SafeMap[string, *sqlQuerierShard]
}

// Creates a new sql sharded querier. Must be closed
func NewSqlQuerier(config *SqlQuerierConfig) (*SqlQuerier, error) {
	sqlQuerier := SqlQuerier{
		replicationStrategy: config.ReplicationStrategy,
		shards:              NewSafeMap[string, *sqlQuerierShard](),
	}

	for _, shard := range config.Shards {
		querierShard, err := newSqlQuerierShard(config.ReplicationStrategy, &shard)
		if err != nil {
			sqlQuerier.Close()

			return nil, err
		}

		sqlQuerier.shards.Set(querierShard.key, querierShard)
	}

	return &sqlQuerier, nil
}

func (d *SqlQuerier) Close() []error {
	var errs []error

	for shardKey, shard := range d.shards.Lookup() {
		if err := shard.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close shard [%s] querier, got error [%s]", shardKey, err.Error()))
		}

		for _, replica := range shard.replicas {
			if db := replica.db; db != nil {
				if err := db.Close(); err != nil {
					errs = append(errs, fmt.Errorf("failed to close replica [%s] querier, got error [%s]", replica.key, err.Error()))
				}
			}
		}
	}

	return errs
}

// ShardKeys Returns all the registered shard keys
func (d *SqlQuerier) ShardKeys() (keys []string) {
	return d.shards.Keys()
}

// Transact Executes a query against a shard and its replicas, it must be used for operations
// in which the database state is changed, such as DML and DDL commands, note that
// [SqlQuerier.Query] will explicit not commit any data to the database
func (d *SqlQuerier) Transact(shardKey, query string, args ...any) error {
	shard, exists := d.shards.Get(shardKey)
	if !exists {
		return fmt.Errorf("shard [%s] is not registered", shardKey)
	}

	switch d.replicationStrategy {
	case ReplicateAll:
		return d.mutateAndReplicateAllSync(shard, true, query, args...)

	case ReplicateNone:
	default:
		return d.mutateAndDontReplicate(shard, query, args...)
	}

	return nil
}

func (d SqlQuerier) mutateAndDontReplicate(shard *sqlQuerierShard, query string, args ...any) error {
	tx, err := shard.db.Begin()
	if err != nil {
		return err
	}

	if _, err := tx.Exec(query, args...); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return errors.Join(err, rollbackErr)
		}

		return err
	}

	return tx.Commit()
}

func (d SqlQuerier) mutateAndReplicateAllSync(shard *sqlQuerierShard, traverseReplicas bool, query string, args ...any) error {
	var (
		txs        []*sql.Tx
		shardErr   error
		replicaErr error
		finalErr   error
	)

	shardTx, err := shard.db.Begin()
	if err != nil {
		return err
	}
	txs = append(txs, shardTx)

	if _, err := shardTx.Exec(query, args...); err != nil {
		shardErr = err
	}

	if shardErr == nil && traverseReplicas {
		for _, replica := range shard.replicas {
			replicaTx, err := replica.db.Begin()
			if err != nil {
				replicaErr = err
				break
			}

			txs = append(txs, replicaTx)

			if _, err := replicaTx.Exec(query, args...); err != nil {
				replicaErr = err
				break
			}
		}
	}

	commitAll := shardErr == nil && replicaErr == nil

	if commitAll {
		for _, tx := range txs {
			if err := tx.Commit(); err != nil {
				finalErr = errors.Join(finalErr, err)
			}
		}
	} else {
		finalErr = errors.Join(shardErr, replicaErr)

		for _, tx := range txs {
			if err := tx.Rollback(); err != nil {
				finalErr = errors.Join(finalErr, err)
			}
		}
	}

	return finalErr
}

func (d SqlQuerier) Query(shardKey, query string, args ...any) (sql.Rows, error) {
	return sql.Rows{}, nil // TODO -> Implementar
}
