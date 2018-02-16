package tables

import (
	"github.com/whosonfirst/go-whosonfirst-sqlite"
)

func CommonTablesWithDatabase(db sqlite.Database) ([]sqlite.Table, error) {

	to_index := make([]sqlite.Table, 0)

	gt, err := NewGeoJSONTableWithDatabase(db)

	if err != nil {
		return nil, err
	}

	to_index = append(to_index, gt)

	st, err := NewSPRTableWithDatabase(db)

	if err != nil {
		return nil, err
	}

	to_index = append(to_index, st)

	nm, err := NewNamesTableWithDatabase(db)

	if err != nil {
		return nil, err
	}

	to_index = append(to_index, nm)

	an, err := NewAncestorsTableWithDatabase(db)

	if err != nil {
		return nil, err
	}

	to_index = append(to_index, an)

	cn, err := NewConcordancesTableWithDatabase(db)

	if err != nil {
		return nil, err
	}

	to_index = append(to_index, cn)

	return to_index, nil
}
