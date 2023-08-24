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
	"crypto/md5"
	"encoding/hex"
	"hash/crc32"
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

	metaMaterial string = "" +
		"CoACa0/B6pPpInXLffXsD4RWzAe7JMK94E9pzmMbT8Uq1E0GyiDJ1ssku+U6U3nPHqeMON83vIoeMWey" +
		"v3jzjEuOgV/cm3IDZe8d6O3mWBZ6F45+LjR/Sc0Gw8Hax3zcsonvTIljWzbiwhEQSw+Wlo5vbkU6IYfS" +
		"7XHtffyNv1znf9FCY6n9da/MbbNDH0z3jbiy8PZgIucFitna8U2UU6l/U4Az3TRhhm/sEwRuONPgQV4i" +
		"+rbCbR0cVJsqaLaRfPCPwO1TlMfEIlpejucZyrHdF2HUanuwjIG6EqAlfLaVY6/Occ+XQlE31GExCMFx" +
		"wk3FYKKkWIg4aKijdOOAsOUfoBo8TAKhluGM6jPoTI/sWPe7rJRqXbVPdSmmzWgPKmxiJ3Pz0Fv1t/Ua" +
		"mhB0HzsvpJHg4Bk4H1Vlyc0vQJH4"
	bodyMaterial string = "" +
		"CoACa0/B6pPpInXLffXsD4RWzAe7JMK94E9pzmMbT8Uq1E0GyiDJ1ssku+U6U3nPHqeMON83vIoeMWey" +
		"v3jzjEuOgV/cm3IDZe8d6O3mWBZ6F45+LjR/Sc0Gw8Hax3zcsonvTIljWzbiwhEQSw+Wlo5vbkU6IYfS" +
		"7XHtffyNv1znf9FCY6n9da/MbbNDH0z3jbiy8PZgIucFitna8U2UU6l/U4Az3TRhhm/sEwRuONPgQV4i" +
		"+rbCbR0cVJsqaLaRfPCPwO1TlMfEIlpejucZyrHdF2HUanuwjIG6EqAlfLaVY6/Occ+XQlE31GExCMFx" +
		"wk3FYKKkWIg4aKijdOOAsOUfoBIQZjxuARdr1C24eHatCjp0vRo8TAKhluGM6jPoTI/sWPe7rJRqXbVP" +
		"dSmmzWgPKmxiJ3Pz0Fv1t/UamhB0HzsvpJHg4Bk4H1Vlyc0vQJH4OAE="

	cryptor   = crypto.NoneCryptor()
	encoder   = newTransmitter()
	requester = newTransFuncEncoder()
	responser = newTransFuncDecoder()
)

func newTransmitter() (t crypto.Transmitter) {
	var err error
	if t, err = cryptor.Transmitter(pass); err != nil {
		panic(err)
	}
	return
}

func newTransFuncEncoder() func(io.Reader) (io.Reader, string) {
	return func(r io.Reader) (io.Reader, string) {
		if pass == "" {
			return r, ""
		}
		rr, newMaterial, err := cryptor.TransEncryptor(bodyMaterial, r)
		if err != nil {
			panic(err)
		}
		return rr, newMaterial
	}
}

func newTransFuncDecoder() func(io.Reader, string) io.Reader {
	return func(r io.Reader, respMaterial string) io.Reader {
		if respMaterial == "" {
			return r
		}
		rr, err := cryptor.TransDecryptor(respMaterial, r)
		if err != nil {
			panic(err)
		}
		return rr
	}
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
						pass = metaMaterial
					}
					cryptor = crypto.NewCryptor()
				} else {
					pass = ""
					cryptor = crypto.NoneCryptor()
				}
				encoder = newTransmitter()
				requester = newTransFuncEncoder()
				responser = newTransFuncDecoder()
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
	uidCommand := &grumble.Command{
		Name: "uid",
		Help: "encode uid root path",
		Args: func(a *grumble.Args) {
			a.String("str", "string")
		},
		Run: func(c *grumble.Context) error {
			uid := c.Args.String("str")
			const hashMask = 1024
			data := md5.Sum([]byte(uid))
			h1 := crc32.ChecksumIEEE(data[:])
			data = md5.Sum([]byte(fmt.Sprintf("%d", h1)))
			h2 := crc32.ChecksumIEEE(data[:])
			fmt.Printf("/%d/%d/%s\n", h1%hashMask, h2%hashMask, uid)
			return nil
		},
	}
	app.AddCommand(userCommand)
	app.AddCommand(hexCommand)
	app.AddCommand(gcmCommand)
	app.AddCommand(uidCommand)
}
