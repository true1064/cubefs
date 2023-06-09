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
	"encoding/hex"
	"io"
	"os"
	"path"

	"github.com/desertbit/grumble"
	"github.com/fatih/color"

	"github.com/cubefs/cubefs/apinode/crypto"
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
	pass string = ""

	material string = "U/ebtje8bQgC9a2PjCwnoq1EpP67EadmyF1I+v8a" +
		"kP5Gx6LTaoIPEY4DQi9+sDsKrWz2ufWRxAgM4anA" +
		"qBUM04TPJnFFHTxJp3NUFV0u35Oh7mtHBPCQ2OJ6" +
		"hjXM7U8ubzyYnZ++t4Kz+234fVZnbAYhyfsKu7y7" +
		"ZlU8grL9/Uvec9mjGMQ2q9dCZdNXqaKnqOEJzDIk" +
		"HBHO4uKV2PxMs8qj9AD+IO6tLVK6n1MN/hr9lNAQ" +
		"+GkAylTyGwbv8n9UAgGXT1HA8D6Yz9ELUv5//YDr" +
		"b4IKkgXznzqBSs7B0iIwIMkwPKSPbLQkkUBX/6AZ" +
		"j/ujkpzETbrcR7EDJOY+5g=="

	cryptor = crypto.NoneCryptor()
	encoder = newTransmitter(true)
	decoder = newTransmitter(false)
)

func newTransmitter(encode bool) (t crypto.Transmitter) {
	var err error
	if encode {
		t, err = cryptor.EncryptTransmitter(pass)
	} else {
		t, err = cryptor.DecryptTransmitter(pass)
	}
	if err != nil {
		panic(err)
	}
	return
}

func registerUser(app *grumble.App) {
	userCommand := &grumble.Command{
		Name:     "var",
		Help:     "set var",
		LongHelp: "local vars: [user, host, pass]",
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
				fmt.Println("pass :", pass != "")
				return nil
			}

			switch key {
			case "host":
				host = val
			case "user":
				user = val
			case "pass":
				if val != "" {
					if len(val) > 16 {
						pass = val
					} else {
						pass = material
					}
					cryptor = crypto.NewCryptor()
				} else {
					pass = ""
					cryptor = crypto.NoneCryptor()
				}
				encoder = newTransmitter(true)
				decoder = newTransmitter(false)
			}
			return nil
		},
	}
	hexCommand := &grumble.Command{
		Name: "hex",
		Help: "encode/decode hex",
		Args: func(a *grumble.Args) {
			a.String("str", "string")
		},
		Flags: func(f *grumble.Flags) {
			f.Bool("e", "encode", false, "encode")
		},
		Run: func(c *grumble.Context) error {
			str := c.Args.String("str")
			if c.Flags.Bool("encode") {
				fmt.Println(hex.EncodeToString([]byte(str)))
				return nil
			}
			b, err := hex.DecodeString(str)
			if err != nil {
				return err
			}
			fmt.Println(string(b))
			return nil
		},
	}
	gcmCommand := &grumble.Command{
		Name: "gcm",
		Help: "encode/decode trans gcm",
		Args: func(a *grumble.Args) {
			a.String("str", "string")
		},
		Flags: func(f *grumble.Flags) {
			f.Bool("e", "encode", false, "encode")
		},
		Run: func(c *grumble.Context) error {
			str := c.Args.String("str")
			if c.Flags.Bool("encode") {
				eVal, err := encoder.Encrypt(str, true)
				if err != nil {
					return err
				}
				fmt.Println(eVal)
				return nil
			}
			dVal, err := encoder.Decrypt(str, true)
			if err != nil {
				return err
			}
			fmt.Println(dVal)
			return nil
		},
	}
	app.AddCommand(userCommand)
	app.AddCommand(hexCommand)
	app.AddCommand(gcmCommand)
}
