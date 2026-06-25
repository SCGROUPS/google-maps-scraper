package placegraphwriter

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gosom/google-maps-scraper/gmaps"
	"github.com/gosom/scrapemate"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PlacegraphWriter is a scrapemate.ResultWriter that writes scraped
// gmaps.Entry values directly into the Placegraph Bronze layer.
// It opens a raw.scrape_runs batch on construction and closes it on Run exit.
type PlacegraphWriter struct {
	pool        *pgxpool.Pool
	scrapeRunID uuid.UUID
	count       atomic.Int64
	hadError    atomic.Bool
}

// New connects to PostgreSQL, creates a raw.scrape_runs row, and returns
// a ready writer. The caller must invoke Run to consume results.
func New(ctx context.Context, dsn string) (*PlacegraphWriter, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("placegraphwriter: connect: %w", err)
	}

	runID := uuid.New()
	_, err = pool.Exec(ctx,
		`INSERT INTO raw.scrape_runs
		     (run_id, source, status, operator, network, started_at)
		 VALUES ($1, 'gmaps_scraper', 'running', 'google-maps-scraper', 'server', now())`,
		runID,
	)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("placegraphwriter: open batch: %w", err)
	}

	return &PlacegraphWriter{pool: pool, scrapeRunID: runID}, nil
}

// Run implements scrapemate.ResultWriter. Drains the results channel,
// writing each entry to raw.gmaps_places. Closes the pool and finalises
// the scrape_runs row when the channel closes.
func (w *PlacegraphWriter) Run(ctx context.Context, in <-chan scrapemate.Result) error {
	defer w.pool.Close()
	defer w.finalize(ctx)

	for result := range in {
		entries, err := toEntries(result.Data)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if err := w.writeOne(ctx, e); err != nil {
				w.hadError.Store(true)
			}
		}
	}
	return nil
}

func (w *PlacegraphWriter) writeOne(ctx context.Context, e *gmaps.Entry) error {
	if e.PlaceID == "" {
		return nil // no stable ID — skip silently
	}

	payload, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal entry %s: %w", e.PlaceID, err)
	}

	sum := md5.Sum(payload)
	hash := fmt.Sprintf("%x", sum)

	// scrapemate cancels the inbound ctx as its shutdown signal while the
	// writer may still be draining buffered results. Using it directly makes
	// the final insert(s) fail client-side with context.Canceled and silently
	// lose rows (notably the only row of a single-query -c=1 run). Detach from
	// cancellation but keep a bound on how long a write may take. Mirrors the
	// same defence in finalize().
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	tag, err := w.pool.Exec(wctx,
		`INSERT INTO raw.gmaps_places
		     (scrape_id, place_id, content_hash, payload, source_url, http_status, scraped_at)
		 VALUES ($1, $2, $3, $4, $5, 200, now())
		 ON CONFLICT (place_id, content_hash) DO NOTHING`,
		w.scrapeRunID, e.PlaceID, hash, payload, e.Link,
	)
	if err != nil {
		return fmt.Errorf("insert gmaps_places %s: %w", e.PlaceID, err)
	}
	if tag.RowsAffected() > 0 {
		w.count.Add(1)
	}
	return nil
}

func (w *PlacegraphWriter) finalize(ctx context.Context) {
	// scrapemate usually cancels the inbound ctx by the time Run() returns,
	// which would make this UPDATE a silent no-op and leave the scrape_runs
	// row stuck at status='running'. Finalise on a fresh, short-lived context
	// so the batch is always closed out. (The pool is still open here: the
	// deferred pool.Close() runs after this, LIFO.)
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
	}
	status := "success"
	if w.hadError.Load() {
		status = "partial"
	}
	_, _ = w.pool.Exec(ctx,
		`UPDATE raw.scrape_runs
		 SET status = $1, finished_at = now(), places_scraped = $2
		 WHERE run_id = $3`,
		status, w.count.Load(), w.scrapeRunID,
	)
}

// toEntries unwraps scrapemate.Result.Data into a slice of *gmaps.Entry.
// Handles both single-entry and slice-of-any forms produced by the scraper.
func toEntries(data any) ([]*gmaps.Entry, error) {
	if slice, ok := data.([]any); ok {
		out := make([]*gmaps.Entry, 0, len(slice))
		for _, v := range slice {
			if e, ok := v.(*gmaps.Entry); ok {
				out = append(out, e)
			}
		}
		return out, nil
	}
	if e, ok := data.(*gmaps.Entry); ok {
		return []*gmaps.Entry{e}, nil
	}
	return nil, fmt.Errorf("placegraphwriter: unexpected data type %T", data)
}
