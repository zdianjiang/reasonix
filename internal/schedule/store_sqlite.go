package schedule

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// sqliteStore is the durable source of truth. Keeping one task per row makes
// future task history and indexes additive migrations rather than JSON rewrites.
type sqliteStore struct{ path string }

func (s sqliteStore) open() (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", s.path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS scheduled_tasks (
 id TEXT PRIMARY KEY, payload BLOB NOT NULL, updated_at INTEGER NOT NULL
)`); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func (s sqliteStore) load() ([]ScheduledTask, error) {
	db, err := s.open()
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.Query(`SELECT payload FROM scheduled_tasks ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []ScheduledTask
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var task ScheduledTask
		if err := json.Unmarshal(raw, &task); err != nil {
			return nil, fmt.Errorf("decode task: %w", err)
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

func (s sqliteStore) replace(tasks []ScheduledTask, now int64) error {
	db, err := s.open()
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM scheduled_tasks`); err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO scheduled_tasks(id, payload, updated_at) VALUES(?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, task := range tasks {
		raw, err := json.Marshal(task)
		if err != nil {
			return err
		}
		if _, err := stmt.Exec(task.ID, raw, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}
