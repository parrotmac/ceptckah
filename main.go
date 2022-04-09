package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx"

	"github.com/bgentry/que-go"
	"github.com/parrotmac/ceptckah/jobs"
)

func main() {
	pgxConfig := &pgx.ConnConfig{
		User:     "postgres",
		Host:     "localhost",
		Port:     5435,
		Database: "postgres",
	}
	tlds := `{"tlds": [".google"]}`

	log.Fatal((&jobs.Handler{
		PGXConnConfig: pgxConfig,
		WorkerCount:   5,
		WorkerMap: map[string]*jobs.JobConfig{
			jobs.KeyUpdateLogs: {
				F:         jobs.UpdateCTLogList,
				Singleton: true,
				Duration:  time.Hour * 24,
			},
			jobs.KeyNewLogMetadata: {
				F: jobs.NewLogMetadata,
			},
			jobs.KeyCheckSTH: {
				F:         jobs.CheckLogSTH,
				Singleton: true,
				Duration:  time.Minute * 5,
			},
			jobs.KeyGetEntries: {
				F: jobs.GetEntries,
			},
			jobs.KeyUpdateMetadata: {
				F:         jobs.RefreshMetadataForEntries,
				Singleton: true,
			},
		},
		OnStart: func(qc *que.Client, pgxPool *pgx.ConnPool, logger *log.Logger) error {
			err := qc.Enqueue(&que.Job{
				Type:  jobs.KeyUpdateLogs,
				Args:  []byte(tlds),
				RunAt: time.Now(),
			})
			if err != nil {
				return err
			}

			// Handles migration
			err = qc.Enqueue(&que.Job{
				Type:  jobs.KeyUpdateMetadata,
				Args:  []byte(tlds),
				RunAt: time.Now(),
			})
			if err != nil {
				return err
			}

			logger.Println("Starting up... waiting for ctrl-C to stop.")
			go http.ListenAndServe(fmt.Sprintf(":%s", os.Getenv("PORT")), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, "Healthy")
			}))

			return nil
		},
		InitSQL: `
			CREATE TABLE IF NOT EXISTS monitored_logs (
				url       text      PRIMARY KEY,
				processed bigint    NOT NULL DEFAULT 0,
				state     integer   NOT NULL DEFAULT 0,
				connect_url text
			);

			CREATE TABLE IF NOT EXISTS cert_store (
				key              bytea                     PRIMARY KEY,
				leaf             bytea                     NOT NULL,
				not_valid_before timestamp with time zone,
				not_valid_after  timestamp with time zone,
				issuer_cn        text,
				needs_update     boolean,
				discovered       timestamptz               NOT NULL DEFAULT now(),
				needs_ckan_backfill boolean
			);

			CREATE TABLE IF NOT EXISTS cert_index (
				key          bytea         NOT NULL,
				domain       text          NOT NULL,

				CONSTRAINT cert_index_pkey PRIMARY KEY (key, domain)
			);

			CREATE TABLE IF NOT EXISTS error_log (
				discovered   timestamptz   NOT NULL DEFAULT now(),
				error        text          NOT NULL
			);
		`,
	}).WorkForever())
}
