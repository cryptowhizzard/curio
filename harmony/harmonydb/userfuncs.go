package harmonydb

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/georgysavva/scany/v2/dbscan"
	"github.com/jackc/pgerrcode"
	"github.com/samber/lo"
	"github.com/yugabyte/pgx/v5"
	"github.com/yugabyte/pgx/v5/pgconn"
)

var errTx = errors.New("cannot use a non-transaction func in a transaction")

const InitialSerializationErrorRetryWait = 200 * time.Millisecond
const maxRetries = 10

// rawStringOnly is _intentionally_private_ to force only basic strings in SQL queries.
type rawStringOnly string

// ---------- Retriable error helpers ----------

func IsErrSerialization(err error) bool {
	var e2 *pgconn.PgError
	return errors.As(err, &e2) && e2.Code == pgerrcode.SerializationFailure // 40001
}

func IsErrDeadlock(err error) bool {
	var e2 *pgconn.PgError
	return errors.As(err, &e2) && e2.Code == pgerrcode.DeadlockDetected // 40P01
}

// Yugabyte sometimes surfaces tablet conflicts/deadlocks/expiry as XX000,
// e.g. "Operation expired", "deadlock", "conflict".
func isYBConflictXX000(err error) bool {
	var e2 *pgconn.PgError
	if !errors.As(err, &e2) {
		return false
	}
	if e2.Code != pgerrcode.InternalError { // "XX000"
		return false
	}
	m := strings.ToLower(e2.Message)
	d := strings.ToLower(e2.Detail)
	return strings.Contains(m, "deadlock") || strings.Contains(d, "deadlock") ||
		strings.Contains(m, "conflict") || strings.Contains(d, "conflict") ||
		strings.Contains(m, "operation expired") || strings.Contains(d, "operation expired")
}

func isRetriable(err error) bool {
	return IsErrSerialization(err) || IsErrDeadlock(err) || isYBConflictXX000(err)
}

// ---------- Non-transactional operations with retries ----------

// Exec executes changes (INSERT, DELETE, UPDATE), with retries on retriable errors.
func (db *DB) Exec(ctx context.Context, sql rawStringOnly, arguments ...any) (count int, err error) {
	if db.usedInTransaction() {
		return 0, errTx
	}

	retryWait := InitialSerializationErrorRetryWait
	for i := 0; i < maxRetries; i++ {
		res, e := db.pgx.Exec(ctx, string(sql), arguments...)
		if e != nil && isRetriable(e) {
			logger.Warnw("Exec retry due to retriable error",
				"attempt", i+1, "max", maxRetries, "err", e.Error())
			time.Sleep(retryWait)
			retryWait *= 2
			err = e
			continue
		}
		return int(res.RowsAffected()), e
	}
	return 0, err
}

type Qry interface {
	Next() bool
	Err() error
	Close()
	Scan(...any) error
	Values() ([]any, error)
}

type Query struct {
	Qry
}

// Query offers Next/Err/Close/Scan/Values, with retries at open time.
func (db *DB) Query(ctx context.Context, sql rawStringOnly, arguments ...any) (*Query, error) {
	if db.usedInTransaction() {
		return &Query{}, errTx
	}

	retryWait := InitialSerializationErrorRetryWait
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		q, e := db.pgx.Query(ctx, string(sql), arguments...)
		if e != nil && isRetriable(e) {
			logger.Warnw("Query retry due to retriable error",
				"attempt", i+1, "max", maxRetries, "err", e.Error())
			time.Sleep(retryWait)
			retryWait *= 2
			lastErr = e
			continue
		}
		return &Query{q}, e
	}
	return &Query{}, lastErr
}

// StructScan allows scanning a single row into a struct.
func (q *Query) StructScan(s any) error {
	return dbscan.ScanRow(s, dbscanRows{q.Qry.(pgx.Rows)})
}

type Row interface {
	Scan(...any) error
}

type rowErr struct{}

func (rowErr) Scan(_ ...any) error { return errTx }

// QueryRow gets 1 row using column order matching, with retries.
// We perform a test Scan into a dummy value to force early error detection,
// then return a fresh QueryRow for the caller to Scan real destinations.
func (db *DB) QueryRow(ctx context.Context, sql rawStringOnly, arguments ...any) Row {
	if db.usedInTransaction() {
		return rowErr{}
	}

	retryWait := InitialSerializationErrorRetryWait
	for i := 0; i < maxRetries; i++ {
		r := db.pgx.QueryRow(ctx, string(sql), arguments...)
		var dummy any
		err := r.Scan(&dummy)
		if err == nil || !isRetriable(err) {
			// Return a fresh row so the caller can scan for real.
			return db.pgx.QueryRow(ctx, string(sql), arguments...)
		}
		logger.Warnw("QueryRow retry due to retriable error",
			"attempt", i+1, "max", maxRetries, "err", err.Error())
		time.Sleep(retryWait)
		retryWait *= 2
	}
	return rowErr{}
}

type dbscanRows struct {
	pgx.Rows
}

func (d dbscanRows) Close() error {
	d.Rows.Close()
	return nil
}
func (d dbscanRows) Columns() ([]string, error) {
	return lo.Map(d.FieldDescriptions(), func(fd pgconn.FieldDescription, _ int) string {
		return fd.Name
	}), nil
}

func (d dbscanRows) NextResultSet() bool {
	return false
}

// Select multiple rows into a slice using name matching, with retries on open and on scanning.
func (db *DB) Select(ctx context.Context, sliceOfStructPtr any, sql rawStringOnly, arguments ...any) error {
	if db.usedInTransaction() {
		return errTx
	}

	retryWait := InitialSerializationErrorRetryWait
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		rows, e := db.pgx.Query(ctx, string(sql), arguments...)
		if e != nil {
			if isRetriable(e) {
				logger.Warnw("Select retry due to retriable error (open)",
					"attempt", i+1, "max", maxRetries, "err", e.Error())
				time.Sleep(retryWait)
				retryWait *= 2
				lastErr = e
				continue
			}
			return e
		}

		err := dbscan.ScanAll(sliceOfStructPtr, dbscanRows{rows})
		rows.Close()
		if err != nil && isRetriable(err) {
			logger.Warnw("Select retry due to retriable error (scan)",
				"attempt", i+1, "max", maxRetries, "err", err.Error())
			time.Sleep(retryWait)
			retryWait *= 2
			lastErr = err
			continue
		}

		return err
	}
	return lastErr
}

// ---------- Transaction support ----------

type Tx struct {
	pgx.Tx
	ctx context.Context
}

// Exec in a transaction.
func (t *Tx) Exec(sql rawStringOnly, arguments ...any) (count int, err error) {
	res, err := t.Tx.Exec(t.ctx, string(sql), arguments...)
	return int(res.RowsAffected()), err
}

// Query in a transaction.
func (t *Tx) Query(sql rawStringOnly, arguments ...any) (*Query, error) {
	q, err := t.Tx.Query(t.ctx, string(sql), arguments...)
	return &Query{q}, err
}

// QueryRow in a transaction.
func (t *Tx) QueryRow(sql rawStringOnly, arguments ...any) Row {
	return t.Tx.QueryRow(t.ctx, string(sql), arguments...)
}

// Select in a transaction.
func (t *Tx) Select(sliceOfStructPtr any, sql rawStringOnly, arguments ...any) error {
	rows, err := t.Query(sql, arguments...)
	if err != nil {
		return fmt.Errorf("scany: query multiple result rows: %w", err)
	}
	defer rows.Close()
	return dbscan.ScanAll(sliceOfStructPtr, dbscanRows{rows.Qry.(pgx.Rows)})
}

// usedInTransaction detects if caller is already inside a transaction.
func (db *DB) usedInTransaction() bool {
	var framePtrs = (&[20]uintptr{})[:]
	framePtrs = framePtrs[:runtime.Callers(3, framePtrs)]
	return lo.Contains(framePtrs, db.BTFP.Load())
}

type TransactionOptions struct {
	RetrySerializationError            bool
	InitialSerializationErrorRetryWait time.Duration
}

type TransactionOption func(*TransactionOptions)

func OptionRetry() TransactionOption {
	return func(o *TransactionOptions) {
		o.RetrySerializationError = true
	}
}

func OptionSerialRetryTime(d time.Duration) TransactionOption {
	return func(o *TransactionOptions) {
		o.InitialSerializationErrorRetryWait = d
	}
}

// BeginTransaction runs f inside a transaction, with optional retries on retriable errors.
func (db *DB) BeginTransaction(ctx context.Context, f func(*Tx) (commit bool, err error), opt ...TransactionOption) (didCommit bool, retErr error) {
	db.BTFPOnce.Do(func() {
		fp := make([]uintptr, 20)
		runtime.Callers(1, fp)
		db.BTFP.Store(fp[0])
	})
	if db.usedInTransaction() {
		return false, errTx
	}

	opts := TransactionOptions{
		RetrySerializationError:            false,
		InitialSerializationErrorRetryWait: InitialSerializationErrorRetryWait,
	}
	for _, o := range opt {
		o(&opts)
	}

	retryWait := opts.InitialSerializationErrorRetryWait
	for {
		comm, err := db.transactionInner(ctx, f)
		if err != nil && opts.RetrySerializationError && isRetriable(err) {
			logger.Warnw("BeginTransaction retry due to retriable error",
				"err", err.Error(), "retryWait", retryWait)
			time.Sleep(retryWait)
			retryWait *= 2
			continue
		}
		return comm, err
	}
}

func (db *DB) transactionInner(ctx context.Context, f func(*Tx) (commit bool, err error)) (didCommit bool, retErr error) {
	tx, err := db.pgx.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	var commit bool
	defer func() {
		if !commit {
			if tmp := tx.Rollback(ctx); tmp != nil {
				retErr = tmp
			}
		}
	}()
	commit, err = f(&Tx{tx, ctx})
	if err != nil {
		return false, err
	}
	if commit {
		err = tx.Commit(ctx)
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// ---------- Other helpers ----------

func IsErrUniqueContraint(err error) bool {
	var e2 *pgconn.PgError
	return errors.As(err, &e2) && e2.Code == pgerrcode.UniqueViolation
}

func IsErrDDLConflict(err error) bool {
	var e2 *pgconn.PgError
	if !errors.As(err, &e2) {
		return false
	}

	ddlConflictCodes := map[string]bool{
		"42710": true, // duplicate_object
		"42712": true, // duplicate_alias
		"42723": true, // duplicate_function
		"42P04": true, // duplicate_database
		"42P06": true, // duplicate_schema
		"42P07": true, // duplicate_table
		"42P05": true, // duplicate_prepared_statement
		"42P03": true, // duplicate_cursor
		"42701": true, // duplicate_column
		"42704": true, // undefined_object
		"42703": true, // undefined_column
		"42883": true, // undefined_function
		"42P01": true, // undefined_table
		"42P02": true, // undefined_parameter
	}

	return ddlConflictCodes[e2.Code]
}

