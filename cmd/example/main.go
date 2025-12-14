package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/gustapinto/shards"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	querier, err := shards.NewSqlQuerier(&shards.SqlQuerierConfig{
		ReplicationStrategy: shards.ReplicateAll,
		Shards: []shards.SqlShard{
			shards.NewSqlShard(
				"db1",
				"pgx",
				"postgresql://db1:db1@localhost:5432/db1?sslmode=disable",
				shards.NewSqlShardReplica(
					"db1-repl1",
					"pgx",
					"postgresql://db2:db2@localhost:5433/db2?sslmode=disable")),
			shards.NewSqlShard(
				"db3",
				"pgx",
				"postgresql://db3:db3@localhost:5434/db3?sslmode=disable"),
		}})
	if err != nil {
		panic(err)
	}

	defer querier.Close()

	for _, shardKey := range querier.ShardKeys() {
		if err := querier.Transact(shardKey, "CREATE TABLE IF NOT EXISTS example ( id BIGSERIAL PRIMARY KEY, foo VARCHAR(255) )"); err != nil {
			panic(err)
		}
	}

	if err := querier.Transact("db1", "INSERT INTO example (foo) VALUES ('bar')"); err != nil {
		panic(err)
	}

	for {
		reader := bufio.NewReader(os.Stdin)
		sql, _ := reader.ReadString('\n')

		fmt.Println(sql)
	}
}
