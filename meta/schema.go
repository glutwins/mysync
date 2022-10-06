package meta

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

type Schema struct {
	Name      string `db:"SCHEMA_NAME"`
	Character string `db:"DEFAULT_CHARACTER_SET_NAME"`
	Collation string `db:"DEFAULT_COLLATION_NAME"`
	Tables    map[string]*Table
}

func (schema *Schema) Fetch(db *sqlx.DB) error {
	now := time.Now()
	defer func() {
		fmt.Println("Fetch", schema.Name, time.Now().Sub(now))
	}()
	var tables []*Table
	err := db.Select(&tables, "select TABLE_NAME, ENGINE, AUTO_INCREMENT, TABLE_COLLATION, TABLE_COMMENT from information_schema.TABLES where TABLE_SCHEMA=?", schema.Name)
	if err != nil {
		panic(err)
	}

	schema.Tables = make(map[string]*Table)
	for _, table := range tables {
		table.Indexs = make(map[string]*Index)
		schema.Tables[table.Name] = table
	}

	var columns []*Column
	if err := db.Select(&columns, "select TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, ORDINAL_POSITION, IS_NULLABLE, COLLATION_NAME, COLUMN_DEFAULT, COLUMN_COMMENT from information_schema.COLUMNS where TABLE_SCHEMA=?", schema.Name); err != nil {
		panic(err)
	}

	for _, column := range columns {
		table := schema.Tables[column.TableName]
		if table == nil {
			continue
		}
		for len(table.Columns) < int(column.Position) {
			table.Columns = append(table.Columns, nil)
		}
		table.Columns[column.Position-1] = column
	}

	var indexs []struct {
		TableName  string `db:"TABLE_NAME"`
		IndexName  string `db:"INDEX_NAME"`
		ColumnName string `db:"COLUMN_NAME"`
		NonUnique  int    `db:"NON_UNIQUE"`
		IndexType  string `db:"INDEX_TYPE"`
		SeqInIndex int    `db:"SEQ_IN_INDEX"`
	}

	if err := db.Select(&indexs, "select TABLE_NAME, INDEX_NAME, COLUMN_NAME, NON_UNIQUE, INDEX_TYPE, SEQ_IN_INDEX from information_schema.STATISTICS where TABLE_SCHEMA=?", schema.Name); err != nil {
		panic(err)
	}

	for _, index := range indexs {
		table := schema.Tables[index.TableName]
		if table == nil {
			continue
		}

		currentIndex, ok := table.Indexs[index.IndexName]
		if !ok {
			currentIndex = &Index{Name: index.IndexName, IsUnique: index.NonUnique != 1, IndexType: index.IndexType}
		}
		for len(currentIndex.ColumnIndex) < index.SeqInIndex {
			currentIndex.ColumnIndex = append(currentIndex.ColumnIndex, -1)
			table.Indexs[index.IndexName] = currentIndex
		}
		currentIndex.ColumnIndex[index.SeqInIndex-1] = table.GetColumnIndex(index.ColumnName)
	}
	return nil
}

func (schema *Schema) GetTable(name string) *Table {
	return schema.Tables[name]
}

func (schema *Schema) MakeDiff(base *Schema) *DiffResult {
	result := &DiffResult{TaskDB: schema.Name, BaseDB: base.Name, MetaDiff: make(map[string]string), Table: make(map[string]*DiffTableResult)}
	if schema.Character != base.Character {
		result.MetaDiff["character"] = fmt.Sprintf("%s!=%s", schema.Character, base.Character)
	}
	if schema.Collation != base.Collation {
		result.MetaDiff["collation"] = fmt.Sprintf("%s!=%s", schema.Collation, base.Collation)
	}
	for _, table := range schema.Tables {
		baseTable := base.GetTable(table.Name)
		if baseTable == nil {
			result.ToDrop = append(result.ToDrop, table.Name)
			continue
		}

		tableDiff := table.MakeDiff(baseTable)
		if !tableDiff.IsNull() {
			result.Table[tableDiff.Name] = tableDiff
		}
	}

	for _, table := range base.Tables {
		if schema.GetTable(table.Name) == nil {
			result.ToCreate = append(result.ToCreate, table.Name)
		}
	}

	return result
}
