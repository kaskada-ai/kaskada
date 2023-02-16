package client

import (
	"context"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"

	"github.com/kaskada/kaskada-ai/wren/ent"
)

const (
	ent_dialect_postgres = "postgres"
	ent_dialect_sqlite   = "sqlite"
)

// NewEntClient creates a new EntClient
func NewEntClient(ctx context.Context, c EntConfig) *ent.Client {
	client, err := ent.Open(c.DriverName(), c.DataSourceName())
	if err != nil {
		log.Fatal().Err(err).Msgf("failed opening connection to %s", c.DriverName())
	}
	if c.dbDialect == ent_dialect_sqlite {
		// Run the auto migration tool when using sqlite
		if err := client.Schema.Create(ctx); err != nil {
			client.Close()
			log.Fatal().Err(err).Msg("failed creating schema resources")
		}
	}

	return client
}

// EntConfig is the information need to connect to a database
type EntConfig struct {
	dbDialect string
	dbHost    *string
	dbName    *string
	dbPath    *string
	dbPass    *string
	dbPort    *int
	dbUser    *string
	inMemory  *bool
	useSSL    *bool
}

func NewEntConfig(dbDialect string, dbName *string, dbHost *string, inMemory *bool, dbPath *string, dbPass *string, dbPort *int, dbUser *string, useSSL *bool) EntConfig {
	if strings.EqualFold(dbDialect, ent_dialect_postgres) {
		dbDialect = ent_dialect_postgres
		if dbName == nil || dbHost == nil || dbPass == nil || dbPort == nil || dbUser == nil || useSSL == nil {
			log.Fatal().Msg("When using dialect `postgres` flags: `db-host`, `db-name`, `db-pass`, `db-port`, `db-use-ssl`, & `db-user` must all be set.")
		}
	} else if strings.EqualFold(dbDialect, ent_dialect_sqlite) {
		dbDialect = ent_dialect_sqlite
		if inMemory == nil {
			log.Fatal().Msg("When using dialect `sqlite` flag: `db-in-memory` must be set.")
		} else if !*inMemory && dbPath == nil {
			log.Fatal().Msg("When using dialect `sqlite`, with `db-in-memory` set `false` flag: `db-path` must be set.")
		}
	} else {
		log.Fatal().Msg("Flag: `db-dialect` must be set to either `postgres` or `sqlite`.")
	}

	return EntConfig{
		dbDialect: dbDialect,
		dbHost:    dbHost,
		dbName:    dbName,
		dbPath:    dbPath,
		dbPass:    dbPass,
		dbPort:    dbPort,
		dbUser:    dbUser,
		inMemory:  inMemory,
		useSSL:    useSSL,
	}
}

func (c EntConfig) DriverName() string {
	switch c.dbDialect {
	case ent_dialect_postgres:
		return "postgres"
	case ent_dialect_sqlite:
		return "sqlite3"
	default:
		log.Fatal().Msgf("undefined driverName for dialect: %s", c.dbDialect)
		return ""
	}
}

func (c EntConfig) DataSourceName() string {
	switch c.dbDialect {
	case ent_dialect_postgres:
		dataSourceName := fmt.Sprintf("user=%s dbname=%s host=%s port=%d password=%s", *c.dbUser, *c.dbName, *c.dbHost, *c.dbPort, *c.dbPass)
		if *c.useSSL {
			dataSourceName += " sslmode=require"
		} else {
			dataSourceName += " sslmode=disable"
		}
		log.Info().Msg("using postgres as backing store")
		return dataSourceName
	case ent_dialect_sqlite:
		if *c.inMemory {
			log.Info().Msg("using in-memory sqlite as backing store")
			return "file:kaskada.db?mode=memory&cache=shared&_fk=1"
		} else {
			dataSourceName := fmt.Sprintf("file:%s?mode=rwc&_fk=1", *c.dbPath)
			if c.dbUser != nil && c.dbPass != nil {
				dataSourceName += fmt.Sprintf("&_auth&_auth_user=%s&_auth_pass=%s", *c.dbUser, *c.dbPass)
			}
			log.Info().Msgf("using sqlite at `%s` as backing store", *c.dbPath)
			return dataSourceName
		}
	default:
		log.Fatal().Msgf("undefined dataSourceName for dialect: %s", c.dbDialect)
		return ""
	}
}
