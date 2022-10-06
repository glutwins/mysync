package meta

import (
	"database/sql"
	"fmt"
)

type Table struct {
	Name      string        `db:"TABLE_NAME"`
	Engine    string        `db:"ENGINE"`
	AutoIncr  sql.NullInt64 `db:"AUTO_INCREMENT"`
	Collation string        `db:"TABLE_COLLATION"`
	Comment   string        `db:"TABLE_COMMENT"`

	Columns []*Column
	Indexs  map[string]*Index
}

func (t Table) GetColumnIndex(columnName string) int {
	for i, col := range t.Columns {
		if col.Name == columnName {
			return i
		}
	}
	return -1
}

func (t Table) MakeDiff(baseTable *Table) *DiffTableResult {
	result := &DiffTableResult{Name: baseTable.Name, MetaDiff: make(map[string]string), ColumnDiff: make(map[string]map[string]string)}
	if t.Engine != baseTable.Engine {
		result.MetaDiff["engine"] = fmt.Sprintf("%s!=%s", t.Engine, baseTable.Engine)
	}
	if t.Collation != baseTable.Collation {
		result.MetaDiff["collation"] = fmt.Sprintf("%s!=%s", t.Collation, baseTable.Collation)
	}

	if len(t.Columns) != len(baseTable.Columns) {
		result.MetaDiff["colcount"] = fmt.Sprintf("%d!=%d", len(t.Columns), len(baseTable.Columns))
	} else {
		for i, column := range baseTable.Columns {
			if t.Columns[i].Name != column.Name {
				idx := fmt.Sprintf("%d", i)
				if _, ok := result.ColumnDiff[idx]; !ok {
					result.ColumnDiff[idx] = make(map[string]string)
				}
				result.ColumnDiff[idx]["name"] = fmt.Sprintf("%s!=%s", t.Columns[i].Name, column.Name)
			}
			if t.Columns[i].RawType != column.RawType {
				idx := fmt.Sprintf("%d", i)
				if _, ok := result.ColumnDiff[idx]; !ok {
					result.ColumnDiff[idx] = make(map[string]string)
				}
				result.ColumnDiff[idx]["type"] = fmt.Sprintf("%s!=%s", t.Columns[i].RawType, column.RawType)
			}
			if t.Columns[i].Nullable != column.Nullable {
				idx := fmt.Sprintf("%d", i)
				if _, ok := result.ColumnDiff[idx]; !ok {
					result.ColumnDiff[idx] = make(map[string]string)
				}
				result.ColumnDiff[idx]["nullable"] = fmt.Sprintf("%s!=%s", t.Columns[i].Nullable, column.Nullable)
			}
			if t.Columns[i].Collation != column.Collation {
				idx := fmt.Sprintf("%d", i)
				if _, ok := result.ColumnDiff[idx]; !ok {
					result.ColumnDiff[idx] = make(map[string]string)
				}
				result.ColumnDiff[idx]["collation"] = fmt.Sprintf("%v!=%v", t.Columns[i].Collation, column.Collation)
			}
			if t.Columns[i].Default != column.Default {
				idx := fmt.Sprintf("%d", i)
				if _, ok := result.ColumnDiff[idx]; !ok {
					result.ColumnDiff[idx] = make(map[string]string)
				}
				result.ColumnDiff[idx]["default"] = fmt.Sprintf("%v!=%v", t.Columns[i].Default, column.Default)
			}
		}
	}

	if len(t.Indexs) != len(baseTable.Indexs) {
		result.MetaDiff["idxcount"] = fmt.Sprintf("%d!=%d", len(t.Indexs), len(baseTable.Indexs))
	}

	for name, baseIndex := range baseTable.Indexs {
		index, ok := t.Indexs[name]
		if !ok {
			result.IndexToCreate = append(result.IndexToCreate, name)
		} else if !index.Equal(baseIndex) {
			result.IndexToModify = append(result.IndexToModify, name)
		}
	}

	return result
}

type Column struct {
	Name      string         `db:"COLUMN_NAME"`
	TableName string         `db:"TABLE_NAME"`
	RawType   string         `db:"COLUMN_TYPE"`
	Nullable  string         `db:"IS_NULLABLE"`
	Collation sql.NullString `db:"COLLATION_NAME"`
	Default   sql.NullString `db:"COLUMN_DEFAULT"`
	Comment   string         `db:"COLUMN_COMMENT"`
	Position  int64          `db:"ORDINAL_POSITION"`
}

type Index struct {
	Name        string
	IsUnique    bool
	IndexType   string
	ColumnIndex []int
}

func (idx Index) Equal(dst *Index) bool {
	if idx.IsUnique != dst.IsUnique {
		return false
	}
	if idx.IndexType != dst.IndexType {
		return false
	}
	if len(idx.ColumnIndex) != len(dst.ColumnIndex) {
		return false
	}
	for i, col := range idx.ColumnIndex {
		if dst.ColumnIndex[i] != col {
			return false
		}
	}
	return true
}
