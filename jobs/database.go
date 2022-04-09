package jobs

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/bgentry/que-go"
	"github.com/jackc/pgx"
)

type dbInitter struct {
	InitSQL            string
	PreparedStatements map[string]string
	OtherStatements    func(*pgx.Conn) error

	// Clearly this won't stop other instances in a race condition, but should at least stop ourselves from hammering ourselves unnecessarily
	runMutex   sync.Mutex
	runAlready bool
}

func (dbi *dbInitter) ensureInitDone(c *pgx.Conn) error {
	dbi.runMutex.Lock()
	defer dbi.runMutex.Unlock()

	if dbi.runAlready {
		return nil
	}

	_, err := c.Exec(dbi.InitSQL)
	if err != nil {
		return err
	}

	dbi.runAlready = true
	return nil
}

func (dbi *dbInitter) AfterConnect(c *pgx.Conn) error {
	if dbi.InitSQL != "" {
		err := dbi.ensureInitDone(c)
		if err != nil {
			return err
		}
	}

	if dbi.OtherStatements != nil {
		err := dbi.OtherStatements(c)
		if err != nil {
			return err
		}
	}

	if dbi.PreparedStatements != nil {
		for n, sql := range dbi.PreparedStatements {
			_, err := c.Prepare(n, sql)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type Handler struct {
	PGXConnConfig *pgx.ConnConfig
	InitSQL       string
	WorkerCount   int
	WorkerMap     map[string]*JobConfig
	OnStart       func(qc *que.Client, pgxPool *pgx.ConnPool, logger *log.Logger) error
	Logger        *log.Logger
	QueueName     string
}

func (h *Handler) WorkForever() error {
	if h.Logger == nil {
		h.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	pgxPool, err := pgx.NewConnPool(pgx.ConnPoolConfig{
		MaxConnections: h.WorkerCount * 2,
		ConnConfig:     *h.PGXConnConfig,
		AfterConnect: (&dbInitter{
			InitSQL: fmt.Sprintf(`
				CREATE TABLE IF NOT EXISTS que_jobs (
					priority    smallint    NOT NULL DEFAULT 100,
					run_at      timestamptz NOT NULL DEFAULT now(),
					job_id      bigserial   NOT NULL,
					job_class   text        NOT NULL,
					args        json        NOT NULL DEFAULT '[]'::json,
					error_count integer     NOT NULL DEFAULT 0,
					last_error  text,
					queue       text        NOT NULL DEFAULT '',

					CONSTRAINT que_jobs_pkey PRIMARY KEY (queue, priority, run_at, job_id)
				);

				CREATE TABLE IF NOT EXISTS cron_metadata (
					id             text                     PRIMARY KEY,
					last_completed timestamp with time zone NOT NULL DEFAULT TIMESTAMP 'EPOCH',
					next_scheduled timestamp with time zone NOT NULL DEFAULT TIMESTAMP 'EPOCH'
				);

				%s
				`, h.InitSQL),
			OtherStatements:    que.PrepareStatements,
			PreparedStatements: map[string]string{},
		}).AfterConnect,
	})
	if err != nil {
		return err
	}

	qc := que.NewClient(pgxPool)

	workerMap := make(que.WorkMap)
	for k, v := range h.WorkerMap {
		workerMap[k] = v.CloneWith(qc, h.Logger).Run
	}

	workers := que.NewWorkerPool(qc, workerMap, h.WorkerCount)
	workers.Queue = h.QueueName

	// Prepare a shutdown function
	shutdown := func() {
		workers.Shutdown()
		pgxPool.Close()
	}

	// Normal exit
	defer shutdown()

	// Or via signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	signal.Notify(sigCh, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("Received %v, starting shutdown...", sig)
		shutdown()
		log.Println("Shutdown complete")
		os.Exit(0)
	}()

	if h.OnStart != nil {
		err = h.OnStart(qc, pgxPool, h.Logger)
		if err != nil {
			return err
		}
	}

	workers.Start()

	// Wait forever
	select {}
}
