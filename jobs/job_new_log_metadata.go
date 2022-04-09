package jobs

import (
	"encoding/json"
	"fmt"
	que "github.com/bgentry/que-go"
	"github.com/jackc/pgx"
	"log"
)

const (
	KeyNewLogMetadata = "new_log_metadata"
)

type LogMetadataJobArgs struct {
	CTLog CTLog
	TLDs  []string
}

func NewLogMetadata(qc *que.Client, logger *log.Logger, job *que.Job, tx *pgx.Tx) error {
	var arg LogMetadataJobArgs
	err := json.Unmarshal(job.Args, &arg)
	if err != nil {
		return err
	}

	l := arg.CTLog

	var dbState int
	err = tx.QueryRow("SELECT state FROM monitored_logs WHERE url = $1 FOR UPDATE", l.URL).Scan(&dbState)
	if err != nil {
		if err == pgx.ErrNoRows {
			_, err = tx.Exec("INSERT INTO monitored_logs (url, connect_url) VALUES ($1, $2)", l.URL, fmt.Sprintf("https://%s", l.URL))
			if err != nil {
				return err
			}
			return ErrImmediateReschedule
		}
		return err
	}

	// if (l.FinalSTH != nil || l.DisqualifiedAt != 0) && dbState == StateActive {
	// 	_, err = tx.Exec("UPDATE monitored_logs SET state = $1 WHERE url = $2", StateIgnore, l.URL)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	dbState = StateIgnore
	// }

	if dbState == StateActive {
		bb, err := json.Marshal(&CheckSTHConf{
			URL:  l.URL,
			TLDs: arg.TLDs,
		})
		if err != nil {
			return err
		}
		err = qc.EnqueueInTx(&que.Job{
			Type: KeyCheckSTH,
			Args: bb,
		}, tx)
		if err != nil {
			return err
		}
	}

	return nil
}
