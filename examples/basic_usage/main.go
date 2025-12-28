package main

import (
	"database/sql"
	"log"

	"github.com/gustapinto/shards"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	err := shards.Register(
		shards.NewShard("db-1", "pgx", "postgresql://db1:db1@localhost:5432/db1?sslmode=disable"),
		shards.NewShard("db-2", "pgx", "postgresql://db2:db2@localhost:5433/db2?sslmode=disable"),
		shards.NewShard("db-3", "pgx", "postgresql://db3:db3@localhost:5434/db3?sslmode=disable"))
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer shards.CloseAll()

	// Using the lower level "DB" APIs, recommended if you are already using database/sql
	for _, s := range shards.Lookup() {
		log.Printf("Running DDL query on shard %s\n", s.Key)

		_, err := shards.DB(s.Key).Exec(`CREATE TABLE IF NOT EXISTS example ( id BIGSERIAL PRIMARY KEY, foo VARCHAR(255) )`)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	// Using the higher level "Querier" API, recommended if you are writing your code from scratch
	// using github.com/gustapinto/shards
	err = shards.On("db-1", "db-2").FailFast().Do(func(s shards.Shard, tx *sql.Tx) (commit bool, err error) {
		log.Printf("Running DML query on shard %s\n", s.Key)

		if _, err := tx.Exec(`INSERT INTO example (foo) VALUES ('bar')`); err != nil {
			return false, err
		}

		return true, nil
	})
	if err != nil {
		log.Fatalln(err.Error())
	}
}
