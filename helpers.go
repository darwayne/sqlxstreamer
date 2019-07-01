package sqlxstreamer

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/satori/uuid"
)

func ConditionalTx(ctx context.Context, raw interface{}) (*sqlx.Tx, error) {
	switch db := raw.(type) {
	case *sqlx.DB:
		tx, err := NewTx(ctx, db)
		if err != nil {
			return nil, err
		}
		return tx, nil
	case *sqlx.Tx:
		return db, nil
	}
	return nil, fmt.Errorf("invalid db type used")
}

func NewTx(ctx context.Context, db *sqlx.DB) (*sqlx.Tx, error) {
	return db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
}

// Commit will attempt to commit the provided resource. Upon a failure to commit, the
// transaction will be rolled back.
func Commit(tx *sqlx.Tx, resource string) error {
	if err := tx.Commit(); err != nil {
		return Rollback(tx, resource, Err(resource, err))
	}

	return nil
}

// Rollback undoes the supplied transaction.
func Rollback(tx *sqlx.Tx, resource string, err error) error {
	if commitErr := tx.Rollback(); commitErr != nil {
		return errors.Wrap(err, Err(resource, commitErr).Error())
	}
	return err
}

func Err(name string, err error) error {
	return errors.Wrapf(err, "error in table: %s", name)
}

func CursorNameGenerator() string {
	return "streamer_cursor_" + strings.ReplaceAll(uuid.NewV4().String(), "-", "_")
}

func LengthOf(item interface{}) int {
	v := reflect.ValueOf(item)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() == reflect.Slice {
		return v.Len()
	}

	return 0
}