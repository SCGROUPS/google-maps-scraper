package cmdmigrate

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"

	"github.com/gosom/google-maps-scraper/migrations"
	"github.com/gosom/google-maps-scraper/saas"
)

var Command = &cli.Command{
	Name:  "migrate",
	Usage: "Run pending database migrations",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "database-url",
			Usage:   "PostgreSQL connection string",
			Value:   "postgres://postgres:postgres@localhost:5432/gmaps_pro?sslmode=disable",
			Sources: cli.EnvVars(saas.EnvDatabaseURL),
		},
	},
	Action: func(_ context.Context, cmd *cli.Command) error {
		dsn := cmd.String("database-url")

		n, err := migrations.RunWithDSN(dsn)
		if err != nil {
			return fmt.Errorf("failed to run migrations: %w", err)
		}

		if n == 0 {
			fmt.Println("No new migrations")
		} else {
			fmt.Printf("Applied %d migration(s)\n", n)
		}

		return nil
	},
}
