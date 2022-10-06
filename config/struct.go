package config

type DiffTaskConfig struct {
	BaseName          string   `yaml:"base_name"`
	BaseConn          string   `yaml:"base_conn"`
	TaskConn          string   `yaml:"task_conn"`
	DiffIncludeRegexp []string `yaml:"diff_include_regexp"`
	DiffExcludeRegexp []string `yaml:"diff_exclude_regexp"`
	DiffIncludes      []string `yaml:"diff_includes"`
	DiffExcludes      []string `yaml:"diff_excludes"`
}

type DiffConfig struct {
	Tasks      []*DiffTaskConfig `yaml:"diff_tasks"`
	Connection map[string]string `yaml:"connection"`
}
