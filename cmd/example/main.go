package main

import (
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

	err = shards.Each(func(shard *shards.Shard) error {
		const query = `CREATE TABLE IF NOT EXISTS example ( id BIGSERIAL PRIMARY KEY, foo VARCHAR(255) )`

		fmt.Printf("Running on shard %+v\n", shard)

		if _, innerErr := shards.On(shard.Key).Exec(query); innerErr != nil {
			return innerErr
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	if _, err := shards.On("db-1").Exec(`INSERT INTO example (foo) VALUES ('bar')`); err != nil {
		panic(err)
	}
}
