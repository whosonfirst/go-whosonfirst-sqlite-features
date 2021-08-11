package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aaronland/go-sqlite/database"
	"github.com/aaronland/go-sqlite/query"
	"github.com/whosonfirst/go-whosonfirst-sqlite-features/flags"
	"log"
	"os"
	"strings"
)

func main() {

	driver := flag.String("driver", "sqlite3", "")
	var dsn = flag.String("dsn", ":memory:", "")
	var is_current = flag.String("is-current", "", "A comma-separated list of valid existential flags (-1,0,1) to filter results according to their 'mz:is_current' property. Multiple flags are evaluated as a nested 'OR' query.")
	var is_ceased = flag.String("is-ceased", "", "A comma-separated list of valid existential flags (-1,0,1) to filter results according to whether or not they have been marked as ceased. Multiple flags are evaluated as a nested 'OR' query.")
	var is_deprecated = flag.String("is-deprecated", "", "A comma-separated list of valid existential flags (-1,0,1) to filter results according to whether or not they have been marked as deprecated. Multiple flags are evaluated as a nested 'OR' query.")
	var is_superseded = flag.String("is-superseded", "", "A comma-separated list of valid existential flags (-1,0,1) to filter results according to whether or not they have been marked as superseded. Multiple flags are evaluated as a nested 'OR' query.")

	var table = flag.String("table", "search", "The name of the SQLite table to query against.")
	var col = flag.String("column", "names_all", "The 'names_*' column to query against. Valid columns are: names_all, names_preferred, names_variant, names_colloquial.")

	flag.Parse()

	ctx := context.Background()

	db, err := database.NewDBWithDriver(ctx, *driver, *dsn)

	if err != nil {
		log.Fatalf("Unable to create database (%s) because %s", *dsn, err)
	}

	defer db.Close()

	conn, err := db.Conn()

	if err != nil {
		log.Fatalf("Failed to connect to database, because %s", err)
	}

	match := fmt.Sprintf("%s MATCH ?", *col)
	query_str := strings.Join(flag.Args(), " ")

	conditions := []string{
		match,
	}

	args := []interface{}{
		query_str,
	}

	existential := map[string]string{
		"is_current":    *is_current,
		"is_ceased":     *is_ceased,
		"is_deprecated": *is_deprecated,
		"is_superseded": *is_superseded,
	}

	for label, ex_flags := range existential {

		if ex_flags == "" {
			continue
		}

		fl_conditions, fl_args, err := flags.ExistentialFlagsToQueryConditions(label, ex_flags)

		if err != nil {
			log.Fatalf("Invalid '%s' flags (%s) %v", label, ex_flags, err)
		}

		conditions = append(conditions, fl_conditions)

		for _, a := range fl_args {
			args = append(args, a)
		}
	}

	where := strings.Join(conditions, " AND ")

	sql := fmt.Sprintf("SELECT id,name FROM %s WHERE %s", *table, where)
	rows, err := conn.Query(sql, args...)

	if err != nil {
		log.Fatalf("Failed to query database (%s) because %s", sql, err)
	}

	defer rows.Close()

	wr, err := query.NewCSVQueryWriter(ctx, os.Stdout)

	if err != nil {
		log.Fatalf("Failed to create query writer, %v", err)
	}

	err = query.WriteRows(ctx, wr, rows)

	if err != nil {
		log.Fatalf("Failed to write rows, %v", err)
	}
}
