package main

import (
	"database/sql"
	"fmt"

	"github.com/gustapinto/shards"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	err := shards.Register(
		shards.NewShard("db-1", "pgx", "postgresql://db1:db1@localhost:5432/db1?sslmode=disable"),
		shards.NewShard("db-2", "pgx", "postgresql://db2:db2@localhost:5433/db2?sslmode=disable"),
		shards.NewShard("db-3", "pgx", "postgresql://db3:db3@localhost:5434/db3?sslmode=disable"))
	if err != nil {
		panic(err)
	}
	defer shards.CloseAll()

	// Using the lower level APIs
	for _, shard := range shards.Lookup() {
		fmt.Printf("Running query on shard %+v\n", shard)

		_, err := shards.DB(shard.Key).Exec(`CREATE TABLE IF NOT EXISTS example ( id BIGSERIAL PRIMARY KEY, foo VARCHAR(255) )`)
		if err != nil {
			panic(err)
		}
	}

	// Using the higher level "On-Key-Do" API
	err = shards.On("db-1", func(_ shards.Shard, tx *sql.Tx) (commit bool, err error) {
		if _, err := tx.Exec(`INSERT INTO example (foo) VALUES ('bar')`); err != nil {
			return false, err
		}

		return true, nil
	})
	if err != nil {
		panic(err)
	}
}
