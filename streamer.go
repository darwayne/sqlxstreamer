package sqlxstreamer

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
)

type SuccessFn func()
type CallBack func(dataType interface{}, fn SuccessFn)
type Fn func(retrieve CallBack)

// Streamer allows you to easily stream a query without having to load all results into memory at once
type Streamer struct {
	fn                  Fn
	size                int
	query               string
	args                []interface{}
	cursorNameGenerator func() string
}

// New generates a Streamer
func New() *Streamer {
	return &Streamer{size: 2000, cursorNameGenerator: CursorNameGenerator}
}

// Args sets the list of args that pertain to the provided query
func (s *Streamer) Args(args ...interface{}) *Streamer {
	s.args = args
	return s
}

// BatchSize sets the size of the results to fetch at a time
func (s *Streamer) BatchSize(size int) *Streamer {
	s.size = size
	return s
}

// CursorName sets the cursor name for the next query .. if none is provided a default will be generated
func (s *Streamer) CursorName(name string) *Streamer {
	s.cursorNameGenerator = func() string {
		return name
	}
	return s
}

// EachBatch calls the provided function each time a batch is retrieved
func (s *Streamer) EachBatch(fn Fn) *Streamer {
	s.fn = fn
	return s
}

// Query sets the query
func (s *Streamer) Query(query string) *Streamer {
	s.query = query
	return s
}

// Do executes the stream query
func (s *Streamer) Do(ctx context.Context, db interface{}) error {
	tx, err := ConditionalTx(ctx, db)
	if err != nil {
		return err
	}

	cursorName := s.cursorNameGenerator()
	query := fmt.Sprintf(`DECLARE %s CURSOR FOR %s`, cursorName, s.query)
	if _, err := tx.ExecContext(ctx, tx.Rebind(query), s.args...); err != nil {
		return errors.Wrap(err, "error declaring cursor")
	}

	idx := 0
	for {
		idx++
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		query := fmt.Sprintf("FETCH %d FROM %s;", s.size, cursorName)
		var data interface{}
		var successFn SuccessFn
		setData := func(dataType interface{}, fn SuccessFn) {
			data = dataType
			successFn = fn
		}

		s.fn(setData)
		err := tx.SelectContext(ctx, data, query)
		if err != nil {
			return err
		}
		resultSize := LengthOf(data)

		if resultSize == 0 && idx > 1 {
			return nil
		}

		successFn()
		if resultSize < s.size {
			return nil
		}
	}
}
