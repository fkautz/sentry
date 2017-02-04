// Copyright © 2017 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"github.com/fkautz/sentry/sentrylib"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
)

// createCmd represents the create command
var createCmd = &cobra.Command{
	Use:   "create",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		config := sentrylib.Config{
			AprsServer:   "noam.aprs2.net:14580",
			AprsUser:     "MYCALL",
			AprsPasscode: "12345",
			AprsFilter:   "s//# s//& s/# s/&",
			Cutoff:       "25h",
			Mailgun: &sentrylib.MailgunConfig{
				Domain:    "example.com",
				ApiKey:    "apikey",
				PubApiKey: "pubapikey",
			},
		}
		if _, err := os.Stat("sentry.yaml"); os.IsNotExist(err) {
			configBytes, err := yaml.Marshal(config)
			if err != nil {
				log.Fatalln(err)
			}
			err = ioutil.WriteFile("sentry.yaml", configBytes, 0600)
			if err != nil {
				log.Fatalln(err)
			}
		} else {
			log.Fatalln("Config file sentry.yaml already exists")
		}
	},
}

func init() {
	configCmd.AddCommand(createCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// createCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// createCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

}
