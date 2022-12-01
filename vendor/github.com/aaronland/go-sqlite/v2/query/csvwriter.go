package query

import (
	"context"
	"encoding/csv"
	"io"
)

type CSVQueryWriter struct {
	QueryWriter
	csv_writer *csv.Writer
}

func NewCSVQueryWriter(ctx context.Context, wr io.Writer) (QueryWriter, error) {

	csv_wr := csv.NewWriter(wr)

	query_wr := &CSVQueryWriter{
		csv_writer: csv_wr,
	}

	return query_wr, nil
}

func (query_wr *CSVQueryWriter) WriteRow(ctx context.Context, row []string) error {

	err := query_wr.csv_writer.Write(row)

	if err != nil {
		return err
	}

	query_wr.csv_writer.Flush()
	return nil
}
