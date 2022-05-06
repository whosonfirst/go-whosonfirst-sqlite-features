package tables

import (
	"fmt"
	"github.com/aaronland/go-sqlite"
)

// TBD: move these in to aaronland/go-sqlite ?

// WrapError returns a new error wrapping 'err' and prepending with the value of 't's Name() method.
func WrapError(t sqlite.Table, err error) error {
	return fmt.Errorf("[%s] %w", t.Name(), err)
}

func InitializeTableError(t sqlite.Table, err error) error {
	return WrapError(t, fmt.Errorf("Failed to initialize database table, %w", err))
}

func MissingPropertyError(t sqlite.Table, prop string, err error) error {
	return WrapError(t, fmt.Errorf("Failed to determine value for '%s' property, %w", err))
}

func DatabaseConnectionError(t sqlite.Table, err error) error {
	return WrapError(t, fmt.Errorf("Failed to establish database connection, %w", err))
}

func BeginTransactionError(t sqlite.Table, err error) error {
	return WrapError(t, fmt.Errorf("Failed to begin database transaction, %w", err))
}

func CommitTransactionError(t sqlite.Table, err error) error {
	return WrapError(t, fmt.Errorf("Failed to commit database transaction, %w", err))
}

func PrepareStatementError(t sqlite.Table, err error) error {
	return WrapError(t, fmt.Errorf("Failed to prepare SQL statement, %w", err))
}

func ExecuteStatementError(t sqlite.Table, err error) error {
	return WrapError(t, fmt.Errorf("Failed to execute SQL statement, %w", err))
}
