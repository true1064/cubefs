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

	material string = "" +
		"CtgCWjk3Rk1zUEJQdlZEeTdFQnlkZDJ1V2NWdVp0enFNOVFTRU5mYmgxb2E5QU5tbHNXeDRnSGxVQVVo" +
		"VU5qbmRZdFRJOTYyUU1oMkx3N1QxRi91dU9pbjZXR1M5V05KaSs2SEdLd0t4STFEaUFPZXVMdml4UUph" +
		"b2hkUkVKNDNHMFhHbU9VN0ZKL0R5MFI2VkwvUnpjWFBjMVEwbGxTRUZMT2NnRU5JQnR4bnl4QzY4L1h3" +
		"MWkvMWFiYWtXNkh1dzdwbzBVSFdGRXZDMHh5VDVycUZ4RW91OGYzbHpBcWx6ZjlGTGxXb24xZVJOUlpj" +
		"QWZpUFFBNHJ1a0NGSVR2WXBua3RKd0pTSUVJVDFhWk9NUlVsTDNQQ0VWanVuTlBGZ0pvRUJ1S2F3dEVu" +
		"MWY1OUZBRnNaMkJNc2ljdG1wUFVSVXMvQUtuRXBUdjZwQU1lM3lhL0hlbFR3PT0="
	bodyMaterial string = "" +
		"CtgCSkRmNXlVRDUzYTkvUy9GRDJhVTBSdGNFU0dOeUNHTEhqWXhUMlN2Ym5CVUp5YXQ4eVdHVDArNVRh" +
		"MlF4M00rZTdoazQ3dFhEYThQK0xSV2NlWC9IbzdML3pzL1dBdSs1Y0dUUUp3c1FhRE5rNGVab25xRXpt" +
		"U2c0SHJ0UkEwWklrVDcrL0ZsOGowOWlNSjkwc2QrY3Rpc3djcEFhcTZORzRFTEorc0RxREVBYjhGTkFi" +
		"TTNUQXMvN2dXelpSM0tIQjB6aG1Wa3pNL1J5anQxSkVQUFpYR0FaVmRTdExFNzRXN21ZVGhySzBXeERG" +
		"ZzhqQ25ZUlRQQXJLMnl0cHA0c01ER1V0MEp1Y2doUlA4UHBuVDlvT2JGY2NsNHAyenFhZjh3aGdoWkFD" +
		"c2JDUjRkZFFCdnNzenZIazc0dnhac0V0OG5CNDlNK0U3WjBmdVRpR2x5K2tBPT0SGGlFWEJZMjBTbnhW" +
		"SStva1RaMG9FSlE9PRgC"

	cryptor   = crypto.NoneCryptor()
	encoder   = newTransmitter()
	requester = newTransFunc(true)
	responser = newTransFunc(false)
)

func newTransmitter() (t crypto.Transmitter) {
	var err error
	if t, err = cryptor.Transmitter(pass); err != nil {
		panic(err)
	}
	return
}

func newTransFunc(encode bool) func(io.Reader) io.Reader {
	f := cryptor.TransDecryptor
	if encode {
		f = cryptor.TransEncryptor
	}
	return func(r io.Reader) io.Reader {
		if pass == "" {
			return r
		}
		rr, err := f(bodyMaterial, r)
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
						pass = material
					}
					cryptor = crypto.NewCryptor()
				} else {
					pass = ""
					cryptor = crypto.NoneCryptor()
				}
				encoder = newTransmitter()
				requester = newTransFunc(true)
				responser = newTransFunc(false)
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
