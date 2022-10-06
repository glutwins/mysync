package meta

import (
	"encoding/json"
	"fmt"
	"mysync/config"
	"regexp"

	"github.com/jmoiron/sqlx"
)

type DiffTask struct {
	cfg        *config.DiffTaskConfig
	IncludeExp []*regexp.Regexp
	ExcludeExp []*regexp.Regexp
	BaseDB     *sqlx.DB
	TaskDB     *sqlx.DB
	BaseSchema Schema
	TaskSchema []*Schema
}

func (task *DiffTask) filterSchema(schema *Schema) bool {
	if task.cfg.BaseConn == task.cfg.TaskConn && task.cfg.BaseName == schema.Name {
		return false
	}

	for _, exp := range task.ExcludeExp {
		if exp.MatchString(schema.Name) {
			return false
		}
	}

	for _, name := range task.cfg.DiffExcludes {
		if name == schema.Name {
			return false
		}
	}

	for _, exp := range task.IncludeExp {
		if exp.MatchString(schema.Name) {
			return true
		}
	}

	for _, name := range task.cfg.DiffIncludes {
		if name == schema.Name {
			return true
		}
	}

	return false
}

func (task *DiffTask) init() error {
	if err := task.BaseDB.Get(&task.BaseSchema, "select SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME from information_schema.SCHEMATA where SCHEMA_NAME=?", task.cfg.BaseName); err != nil {
		panic(err)
	}

	if err := task.BaseSchema.Fetch(task.BaseDB); err != nil {
		panic(err)
	}

	var schemas []*Schema
	task.TaskDB.Select(&schemas, "select SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME from information_schema.SCHEMATA")
	for _, schema := range schemas {
		if task.filterSchema(schema) {
			if err := schema.Fetch(task.TaskDB); err != nil {
				panic(err)
			}
			task.TaskSchema = append(task.TaskSchema, schema)
		}
	}

	return nil
}

func (task *DiffTask) MakeDiff() {
	var results []*DiffResult
	for _, schema := range task.TaskSchema {
		if result := schema.MakeDiff(&task.BaseSchema); !result.IsNull() {
			results = append(results, result)
		}
	}
	b, _ := json.Marshal(results)
	fmt.Println(string(b))
}

func NewDiffTask(cfg *config.DiffTaskConfig, dbs map[string]*sqlx.DB) *DiffTask {
	task := &DiffTask{cfg: cfg}
	for _, expstr := range cfg.DiffIncludeRegexp {
		task.IncludeExp = append(task.IncludeExp, regexp.MustCompile(expstr))
	}

	for _, expstr := range cfg.DiffExcludeRegexp {
		task.ExcludeExp = append(task.ExcludeExp, regexp.MustCompile(expstr))
	}

	task.BaseDB = dbs[cfg.BaseConn]
	task.TaskDB = dbs[cfg.TaskConn]

	if err := task.init(); err != nil {
		panic(err)
	}

	return task
}
