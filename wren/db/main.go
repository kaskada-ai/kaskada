package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	_ "ariga.io/atlas/sql/postgres"
	"ariga.io/atlas/sql/sqltool"
	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql/schema"
	"github.com/kouhin/envflag"
	_ "github.com/lib/pq"

	"github.com/kaskada/kaskada-ai/wren/ent/migrate"
)

var (
	// db configuration
	dbHost = flag.String("db-host", "127.0.0.1", "postgres database hostname")
	dbName = flag.String("db-name", "postgres", "postgres database name")
	dbPass = flag.String("db-pass", "mpostgres123", "postgres database password")
	// the migration tool works off a stand-alone test database
	// that is why this it references port 5433 instead of the standard 5432
	// by default
	dbPort   = flag.Int("db-port", 5433, "postgres database port")
	dbUseSSL = flag.Bool("db-use-ssl", false, "the ssl mode to use when connecting to postgres")
	dbUser   = flag.String("db-user", "postgres", "postgres database username")

	// other config
	name = flag.String("name", "", "a name for the migration reason")
)

func main() {
	flag.Usage = func() {
		_, _ = fmt.Fprintf(flag.CommandLine.Output(), "generates db migrations from the ent schema")
		flag.PrintDefaults()
	}

	if err := envflag.Parse(); err != nil {
		log.Fatalf("failed to parse flags: %v", err)
	}

	// We need a name for the new migration file.
	if *name == "" {
		log.Fatalln("no name given. run with 'make' like this: 'NAME=the_name make ent/create-migrations'")
	}

	// Get the local migrations directory.
	dir, err := sqltool.NewGolangMigrateDir("wren/db/migrations")
	if err != nil {
		log.Fatalf("failed getting atlas migration directory: %v", err)
	}

	// connect to database
	connectionString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", *dbUser, *dbPass, *dbHost, *dbPort, *dbName)
	if *dbUseSSL {
		connectionString += "?sslmode=require"
	} else {
		connectionString += "?sslmode=disable"
	}

	// Write migration diff.
	opts := []schema.MigrateOption{
		schema.WithDialect(dialect.Postgres), // Ent dialect to use
		schema.WithDir(dir),                  // provide migration directory
		schema.WithDropColumn(true),
		schema.WithDropIndex(true),
		schema.WithFormatter(sqltool.GolangMigrateFormatter),
		schema.WithMigrationMode(schema.ModeReplay), // provide migration mode
	}

	// Generate migrations using Atlas
	err = migrate.NamedDiff(context.Background(), connectionString, *name, opts...)
	if err != nil {
		log.Fatalf("failed generating migration files: %v", err)
	}

}
