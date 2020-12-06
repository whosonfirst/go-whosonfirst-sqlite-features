package tables

// https://www.sqlite.org/rtree.html

import (
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/properties/whosonfirst"
	"github.com/whosonfirst/go-whosonfirst-sqlite"
	"github.com/whosonfirst/go-whosonfirst-sqlite-features"
	"github.com/whosonfirst/go-whosonfirst-sqlite/utils"
	_ "log"
)

type RTreeTableOptions struct {
	IndexAltFiles bool
}

func DefaultRTreeTableOptions() (*RTreeTableOptions, error) {

	opts := RTreeTableOptions{
		IndexAltFiles: false,
	}

	return &opts, nil
}

type RTreeTable struct {
	features.FeatureTable
	name    string
	options *RTreeTableOptions
}

type RTreeRow struct {
	Id           int64
	MinX float64
	MinY float64
	MaxX float64
	MaxY float64
	LastModified int64
}

func NewRTreeTable() (sqlite.Table, error) {

	opts, err := DefaultRTreeTableOptions()

	if err != nil {
		return nil, err
	}

	return NewRTreeTableWithOptions(opts)
}

func NewRTreeTableWithOptions(opts *RTreeTableOptions) (sqlite.Table, error) {

	t := RTreeTable{
		name:    "rtree",
		options: opts,
	}

	return &t, nil
}

func NewRTreeTableWithDatabase(db sqlite.Database) (sqlite.Table, error) {

	opts, err := DefaultRTreeTableOptions()

	if err != nil {
		return nil, err
	}

	return NewRTreeTableWithDatabaseAndOptions(db, opts)
}

func NewRTreeTableWithDatabaseAndOptions(db sqlite.Database, opts *RTreeTableOptions) (sqlite.Table, error) {

	t, err := NewRTreeTableWithOptions(opts)

	if err != nil {
		return nil, err
	}

	err = t.InitializeTable(db)

	if err != nil {
		return nil, err
	}

	return t, nil
}

func (t *RTreeTable) Name() string {
	return t.name
}

func (t *RTreeTable) Schema() string {

	sql := `CREATE VIRTUAL TABLE %s USING rtree (
		id INTEGER NOT NULL PRIMARY KEY,
		is_alt TINYINT,
		min_x DECIMAL,
		min_y DECIMAL,
		max_x DECIMAL,
		max_y DECIMAL,
		lastmodified INTEGER
	);

	CREATE INDEX rtree_by_lastmod ON %s (lastmodified);`

	return fmt.Sprintf(sql, t.Name(), t.Name(), t.Name(), t.Name())
}

func (t *RTreeTable) InitializeTable(db sqlite.Database) error {

	return utils.CreateTableIfNecessary(db, t)
}

func (t *RTreeTable) IndexRecord(db sqlite.Database, i interface{}) error {
	return t.IndexFeature(db, i.(geojson.Feature))
}

func (t *RTreeTable) IndexFeature(db sqlite.Database, f geojson.Feature) error {

	conn, err := db.Conn()

	if err != nil {
		return err
	}

	str_id := f.Id()
	is_alt := whosonfirst.IsAlt(f)

	if is_alt && !t.options.IndexAltFiles {
		return nil
	}

	lastmod := whosonfirst.LastModified(f)

	bboxes, err := f.BoundingBoxes()

	if err != nil {
		return err
	}
	
	tx, err := conn.Begin()

	if err != nil {
		return err
	}

	sql := fmt.Sprintf(`INSERT OR REPLACE INTO %s (
		id, is_alt, min_x, min_y, max_x, max_y, lastmodified
	) VALUES (
		?, ?, ?, ?, ?, ?, ?
	)`, t.Name())

	stmt, err := tx.Prepare(sql)

	if err != nil {
		return err
	}

	defer stmt.Close()

	for _, bbox := range bboxes.Bounds() {

		sw := bbox.Min
		ne := bbox.Max
	
		_, err = stmt.Exec(str_id, is_alt, sw.X, sw.Y, ne.X, ne.Y, lastmod)

		if err != nil {
			return err
		}
	}
	
	return tx.Commit()
}
