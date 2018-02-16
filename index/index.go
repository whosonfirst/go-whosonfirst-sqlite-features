package index

import (
	"context"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/feature"
	wof_index "github.com/whosonfirst/go-whosonfirst-index"
	wof_utils "github.com/whosonfirst/go-whosonfirst-index/utils"
	sql_index "github.com/whosonfirst/go-whosonfirst-sqlite/index"
	"io"
)

// THIS IS A TOTAL HACK UNTIL WE CAN SORT THINGS OUT IN
// go-whosonfirst-index... (20180206/thisisaaronland)

type Closer struct {
	fh io.Reader
}

func (c Closer) Read(b []byte) (int, error) {
	return c.fh.Read(b)
}

func (c Closer) Close() error {
	return nil
}

func NewDefaultSQLiteFeaturesIndexer(db sqlite.Database, to_index []sqlite.Table) (sql_index.Index, error) {

	cb := func(ctx context.Context, fh io.Reader, args ...interface{}) (interface{}, error) {

		path, err := wof_index.PathForContext(ctx)

		if err != nil {

			/*
				if *liberal {
					return nil, nil
				}
			*/

			return nil, err
		}

		ok, err := wof_utils.IsPrincipalWOFRecord(fh, ctx)

		if err != nil {

			/*
				if *liberal {
					return nil, nil
				}
			*/

			return nil, err
		}

		if !ok {
			return nil, nil
		}

		// HACK - see above
		closer := Closer{fh}

		i, err := feature.LoadWOFFeatureFromReader(closer)

		if err != nil {

			logger.Warning("failed to index %s because %s", path, err)

			if *liberal {
				return nil, nil
			}

			return nil, err
		}

		return i, nil
	}

	return sql_index.NewSQLiteIndexer(db, to_index, cb)
}
