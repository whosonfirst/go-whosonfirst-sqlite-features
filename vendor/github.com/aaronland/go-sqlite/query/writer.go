package query

import (
	"context"
	"database/sql"
	"fmt"
)

type QueryWriter interface {
	WriteRow(context.Context, []string) error
}

// The guts of this (minus the QueryWriter stuff) are copied from:
// https://github.com/psanford/sqlite3vfshttp/blob/main/sqlitehttpcli/sqlitehttpcli.go

func WriteRows(ctx context.Context, wr QueryWriter, rows *sql.Rows) error {

	cols, err := rows.Columns()

	if err != nil {
		return fmt.Errorf("Failed to determine columns for rows, %v", err)
	}

	for rows.Next() {

		rows.Columns()

		columns := make([]*string, len(cols))

		columnPointers := make([]interface{}, len(cols))

		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		err = rows.Scan(columnPointers...)

		if err != nil {
			return fmt.Errorf("Failed to scan row, %w", err)
		}

		names := make([]string, 0, len(columns))

		for _, col := range columns {
			if col == nil {
				names = append(names, "NULL")
			} else {
				names = append(names, *col)
			}
		}

		err = wr.WriteRow(ctx, names)

		if err != nil {
			return fmt.Errorf("Failed to write row, %v", err)
		}
	}

	err = rows.Close()

	if err != nil {
		return fmt.Errorf("Failed to close rows, %v", err)
	}

	return nil
}
