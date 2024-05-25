package tables

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aaronland/go-sqlite/v2"
	"github.com/whosonfirst/go-whosonfirst-feature/alt"
	"github.com/whosonfirst/go-whosonfirst-feature/properties"
	"github.com/whosonfirst/go-whosonfirst-spelunker/document"
	sql_tables "github.com/whosonfirst/go-whosonfirst-sql/tables"
	"github.com/whosonfirst/go-whosonfirst-sqlite-features/v2"
)

type SpelunkerTableOptions struct {
	IndexAltFiles          bool
	AllowMissingSourceGeom bool
}

func DefaultSpelunkerTableOptions() (*SpelunkerTableOptions, error) {

	opts := SpelunkerTableOptions{
		IndexAltFiles:          false,
		AllowMissingSourceGeom: true,
	}

	return &opts, nil
}

type SpelunkerTable struct {
	features.FeatureTable
	name    string
	options *SpelunkerTableOptions
}

type SpelunkerRow struct {
	Id           int64
	Body         string
	LastModified int64
}

func NewSpelunkerTableWithDatabase(ctx context.Context, db sqlite.Database) (sqlite.Table, error) {

	opts, err := DefaultSpelunkerTableOptions()

	if err != nil {
		return nil, err
	}

	return NewSpelunkerTableWithDatabaseAndOptions(ctx, db, opts)
}

func NewSpelunkerTableWithDatabaseAndOptions(ctx context.Context, db sqlite.Database, opts *SpelunkerTableOptions) (sqlite.Table, error) {

	t, err := NewSpelunkerTableWithOptions(ctx, opts)

	if err != nil {
		return nil, err
	}

	err = t.InitializeTable(ctx, db)

	if err != nil {
		return nil, err
	}

	return t, nil
}

func NewSpelunkerTable(ctx context.Context) (sqlite.Table, error) {

	opts, err := DefaultSpelunkerTableOptions()

	if err != nil {
		return nil, err
	}

	return NewSpelunkerTableWithOptions(ctx, opts)
}

func NewSpelunkerTableWithOptions(ctx context.Context, opts *SpelunkerTableOptions) (sqlite.Table, error) {

	t := SpelunkerTable{
		name:    sql_tables.SPELUNKER_TABLE_NAME,
		options: opts,
	}

	return &t, nil
}

func (t *SpelunkerTable) Name() string {
	return t.name
}

func (t *SpelunkerTable) Schema() string {
	schema, _ := sql_tables.LoadSchema("sqlite", sql_tables.SPELUNKER_TABLE_NAME)
	return schema
}

func (t *SpelunkerTable) InitializeTable(ctx context.Context, db sqlite.Database) error {

	return sqlite.CreateTableIfNecessary(ctx, db, t)
}

func (t *SpelunkerTable) IndexRecord(ctx context.Context, db sqlite.Database, i interface{}) error {
	return t.IndexFeature(ctx, db, i.([]byte))
}

func (t *SpelunkerTable) IndexFeature(ctx context.Context, db sqlite.Database, f []byte) error {

	is_alt := alt.IsAlt(f)

	if is_alt {
		return nil
	}

	id, err := properties.Id(f)

	if err != nil {
		return MissingPropertyError(t, "id", err)
	}

	lastmod := properties.LastModified(f)

	doc, err := document.PrepareSpelunkerV2Document(ctx, f)

	if err != nil {
		return fmt.Errorf("Failed to prepare spelunker document, %w", err)
	}

	enc_doc, err := json.Marshal(doc)

	if err != nil {
		return fmt.Errorf("Failed to marshal spelunker document, %w", err)
	}

	conn, err := db.Conn(ctx)

	if err != nil {
		return DatabaseConnectionError(t, err)
	}

	tx, err := conn.Begin()

	if err != nil {
		return BeginTransactionError(t, err)
	}

	sql := fmt.Sprintf(`INSERT OR REPLACE INTO %s (
		id, body, lastmodified
	) VALUES (
		?, ?, ?
	)`, t.Name())

	stmt, err := tx.Prepare(sql)

	if err != nil {
		return PrepareStatementError(t, err)
	}

	defer stmt.Close()

	str_doc := string(enc_doc)

	_, err = stmt.Exec(id, str_doc, lastmod)

	if err != nil {
		return ExecuteStatementError(t, err)
	}

	err = tx.Commit()

	if err != nil {
		return CommitTransactionError(t, err)
	}

	return nil
}
