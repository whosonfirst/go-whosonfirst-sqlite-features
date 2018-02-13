package main

import (
	"flag"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-log"
	"github.com/whosonfirst/go-whosonfirst-sqlite/database"
	"github.com/whosonfirst/go-whosonfirst-sqlite/utils"
	"io"
	"os"
	"strings"
)

func main() {

	driver := flag.String("driver", "sqlite3", "")
	var dsn = flag.String("dsn", "index.db", "")
	var is_current = flag.String("is-current", "", "A comma-separated list of valid existential flags (-1,0,1) to filter results according to their 'mz:is_current' property. Multiple flags are evaluated as a nested 'OR' query.")
	var is_ceased = flag.String("is-ceased", "", "A comma-separated list of valid existential flags (-1,0,1) to filter results according to whether or not they have been marked as ceased. Multiple flags are evaluated as a nested 'OR' query.")
	var is_deprecated = flag.String("is-deprecated", "", "A comma-separated list of valid existential flags (-1,0,1) to filter results according to whether or not they have been marked as deprecated. Multiple flags are evaluated as a nested 'OR' query.")
	var is_superseded = flag.String("is-superseded", "", "A comma-separated list of valid existential flags (-1,0,1) to filter results according to whether or not they have been marked as superseded. Multiple flags are evaluated as a nested 'OR' query.")

	var table = flag.String("table", "search", "")
	var col = flag.String("column", "names_all", "")

	flag.Parse()

	logger := log.SimpleWOFLogger()

	stdout := io.Writer(os.Stdout)
	logger.AddLogger(stdout, "status")

	db, err := database.NewDBWithDriver(*driver, *dsn)

	if err != nil {
		logger.Fatal("unable to create database (%s) because %s", *dsn, err)
	}

	defer db.Close()

	conn, err := db.Conn()

	if err != nil {
		logger.Fatal("CONN", err)
	}

	match := fmt.Sprintf("%s MATCH ?", *col)
	query := strings.Join(flag.Args(), " ")

	conditions := []string{
		match,
	}

	args := []interface{}{
		query,
	}

	existential := map[string]string{
		"is_current":    *is_current,
		"is_ceased":     *is_ceased,
		"is_deprecated": *is_deprecated,
		"is_superseded": *is_superseded,
	}

	for label, flags := range existential {

		if flags == "" {
			continue
		}

		fl_conditions, fl_args, err := utils.ExistentialFlagsToQueryConditions(label, flags)

		if err != nil {
			logger.Fatal("Invalid '%s' flags (%s) %s", label, flags, err)
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
		logger.Fatal("QUERY", err)
	}

	defer rows.Close()

	logger.Status("# %s", sql)

	for rows.Next() {

		var id string
		var name string

		err = rows.Scan(&id, &name)

		if err != nil {
			logger.Fatal("ID", err)
		}

		logger.Status("%s %s", id, name)
	}

	err = rows.Err()

	if err != nil {
		logger.Fatal("ROWS", err)
	}

}
