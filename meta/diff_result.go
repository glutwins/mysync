package meta

type DiffResult struct {
	TaskDB   string                      `json:"task_db"`
	BaseDB   string                      `json:"base_db"`
	MetaDiff map[string]string           `json:"meta_diff,omitempty"`
	ToCreate []string                    `json:"to_create,omitempty"`
	ToDrop   []string                    `json:"to_drop,omitempty"`
	Table    map[string]*DiffTableResult `json:"table,omitempty"`
}

func (r DiffResult) IsNull() bool {
	return len(r.MetaDiff) == 0 && len(r.ToCreate) == 0 && len(r.ToDrop) == 0 && len(r.Table) == 0
}

type DiffTableResult struct {
	Name          string                       `json:"name"`
	MetaDiff      map[string]string            `json:"meta_diff,omitempty"`
	IndexToCreate []string                     `json:"index_to_create,omitempty"`
	IndexToModify []string                     `json:"index_to_modify,omitempty"`
	ColumnDiff    map[string]map[string]string `json:"column_diff,omitempty"`
}

func (r DiffTableResult) IsNull() bool {
	return !(len(r.MetaDiff) > 0 || len(r.IndexToCreate) > 0 || len(r.IndexToModify) > 0 || len(r.ColumnDiff) > 0)
}
