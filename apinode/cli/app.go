// Copyright 2023 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cli

import (
	"io"
	"os"
	"path"

	"github.com/desertbit/grumble"
	"github.com/fatih/color"

	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

// App command app
var App = grumble.New(&grumble.Config{
	Name:                  "apinode manager",
	Description:           "A command manager of apinode",
	HistoryFile:           path.Join(os.TempDir(), ".apinode_cli.history"),
	HistoryLimit:          10000,
	ErrorColor:            color.New(color.FgRed, color.Bold, color.Faint),
	HelpHeadlineColor:     color.New(color.FgGreen),
	HelpHeadlineUnderline: false,
	HelpSubCommands:       true,
	Prompt:                "API $> ",
	PromptColor:           color.New(color.FgBlue, color.Bold),
	Flags: func(f *grumble.Flags) {
		f.BoolL("silence", false, "disable print output")
	},
})

func init() {
	App.OnInit(func(a *grumble.App, fm grumble.FlagMap) error {
		if fm.Bool("silence") {
			color.Output = io.Discard
			fmt.SetOutput(io.Discard)
			log.SetOutput(io.Discard)
		}
		// build-in flag in grumble
		if fm.Bool("nocolor") {
			color.NoColor = true
		}
		return nil
	})

	registerUser(App)
	registerDrive(App)
}

var (
	host string = "http://localhost:9999"
	user string = "test"
)

func registerUser(app *grumble.App) {
	userCommand := &grumble.Command{
		Name:     "var",
		Help:     "set var",
		LongHelp: "local vars: [user, host]",
		Args: func(a *grumble.Args) {
			a.String("key", "key", grumble.Default(""))
			a.String("val", "value", grumble.Default(""))
		},
		Run: func(c *grumble.Context) error {
			key := c.Args.String("key")
			val := c.Args.String("val")

			if key == "" && val == "" {
				fmt.Println("host :", host)
				fmt.Println("user :", user)
				return nil
			}

			switch key {
			case "host":
				host = val
			case "user":
				user = val
			}
			return nil
		},
	}
	app.AddCommand(userCommand)
}
