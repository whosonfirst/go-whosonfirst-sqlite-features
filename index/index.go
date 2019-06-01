package index

import (
	"context"
	"errors"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/feature"
	wof_index "github.com/whosonfirst/go-whosonfirst-index"
	"github.com/whosonfirst/go-whosonfirst-sqlite"
	sql_index "github.com/whosonfirst/go-whosonfirst-sqlite/index"
	"github.com/whosonfirst/warning"
	"io"
	"io/ioutil"
)

func NewDefaultSQLiteFeaturesIndexer(db sqlite.Database, to_index []sqlite.Table) (*sql_index.SQLiteIndexer, error) {

	cb := func(ctx context.Context, fh io.Reader, args ...interface{}) (interface{}, error) {

		select {

		case <-ctx.Done():
			return nil, nil
		default:
			path, err := wof_index.PathForContext(ctx)

			if err != nil {
				return nil, err
			}

			closer := ioutil.NopCloser(fh)

			// i, err := feature.LoadWOFFeatureFromReader(closer)
			i, err := feature.LoadGeoJSONFeatureFromReader(closer)			

			if err != nil && !warning.IsWarning(err) {
				msg := fmt.Sprintf("Unable to load %s, because %s", path, err)
				return nil, errors.New(msg)
			}

			return i, nil
		}
	}

	return sql_index.NewSQLiteIndexer(db, to_index, cb)
}
