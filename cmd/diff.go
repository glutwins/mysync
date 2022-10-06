/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"mysync/config"
	"mysync/meta"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// diffCmd represents the diff command
var diffCmd = &cobra.Command{
	Use:   "diff",
	Short: "MySQL对比",
	Long:  `根据配置，生产多个MySQL表结构差异`,
	Run: func(cmd *cobra.Command, args []string) {
		cfg := &config.DiffConfig{}
		viper.Unmarshal(cfg, func(m *mapstructure.DecoderConfig) {
			m.TagName = "yaml"
		})

		dbs := make(map[string]*sqlx.DB)
		for name, dsn := range cfg.Connection {
			dbs[name] = sqlx.MustConnect("mysql", dsn)
		}

		wg := &sync.WaitGroup{}
		wg.Add(len(cfg.Tasks))
		for _, taskcfg := range cfg.Tasks {
			go func(cfg *config.DiffTaskConfig) {
				defer wg.Done()
				meta.NewDiffTask(cfg, dbs).MakeDiff()
			}(taskcfg)
		}
		wg.Wait()
	},
}

func init() {
	rootCmd.AddCommand(diffCmd)
}
