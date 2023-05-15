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
	"bytes"
	"io"
	"os"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
)

func addCmdDrive(cmd *grumble.Command) {
	driveCommand := &grumble.Command{
		Name: "drive",
		Help: "drive api",
	}
	cmd.AddCommand(driveCommand)

	driveCommand.AddCommand(&grumble.Command{
		Name: "create",
		Help: "create user drive",
		Run: func(c *grumble.Context) error {
			return show(cli.DriveCreate())
		},
	})
	driveCommand.AddCommand(&grumble.Command{
		Name: "get",
		Help: "get user drive",
		Run: func(c *grumble.Context) error {
			return show(cli.DriveGet())
		},
	})
}

func addCmdConfig(cmd *grumble.Command) {
	configCommand := &grumble.Command{
		Name: "config",
		Help: "config api",
	}
	cmd.AddCommand(configCommand)

	configCommand.AddCommand(&grumble.Command{
		Name: "add",
		Help: "add config",
		Args: func(a *grumble.Args) {
			a.String("path", "path name")
		},
		Run: func(c *grumble.Context) error {
			return cli.ConfigAdd(c.Args.String("path"))
		},
	})
	configCommand.AddCommand(&grumble.Command{
		Name: "get",
		Help: "get config",
		Run: func(c *grumble.Context) error {
			return show(cli.ConfigGet())
		},
	})
	configCommand.AddCommand(&grumble.Command{
		Name: "del",
		Help: "del config",
		Args: func(a *grumble.Args) {
			a.String("path", "path name")
		},
		Run: func(c *grumble.Context) error {
			return cli.ConfigDel(c.Args.String("path"))
		},
	})
}

func addCmdMeta(cmd *grumble.Command) {
	metaCommand := &grumble.Command{
		Name: "meta",
		Help: "meta api",
	}
	cmd.AddCommand(metaCommand)

	metaCommand.AddCommand(&grumble.Command{
		Name: "set",
		Help: "set meta",
		Args: func(a *grumble.Args) {
			a.String("path", "path name")
			a.StringList("meta", "meta with key1 value1 key2 value2")
		},
		Run: func(c *grumble.Context) error {
			return cli.MetaSet(c.Args.String("path"), c.Args.StringList("meta")...)
		},
	})
	metaCommand.AddCommand(&grumble.Command{
		Name: "get",
		Help: "get meta",
		Args: func(a *grumble.Args) {
			a.String("path", "path name")
		},
		Run: func(c *grumble.Context) error {
			return show(cli.MetaGet(c.Args.String("path")))
		},
	})
}

func addCmdDir(cmd *grumble.Command) {
	dirCommand := &grumble.Command{
		Name: "dir",
		Help: "dir api",
	}
	cmd.AddCommand(dirCommand)

	dirCommand.AddCommand(&grumble.Command{
		Name: "list",
		Help: "list dir",
		Flags: func(f *grumble.Flags) {
			f.StringL("path", "/", "path")
			f.StringL("marker", "", "marker")
			f.StringL("limit", "10", "limit")
			f.StringL("filter", "", "filter")
		},
		Run: func(c *grumble.Context) error {
			f := c.Flags.String
			return show(cli.ListDir(f("path"), f("marker"), f("limit"), f("filter")))
		},
	})
	dirCommand.AddCommand(&grumble.Command{
		Name: "make",
		Help: "make dir",
		Flags: func(f *grumble.Flags) {
			f.StringL("path", "", "path")
			f.BoolL("recursive", false, "recursive")
		},
		Run: func(c *grumble.Context) error {
			return cli.MkDir(c.Flags.String("path"), c.Flags.Bool("recursive"))
		},
	})
}

func addCmdFile(cmd *grumble.Command) {
	fileCommand := &grumble.Command{
		Name: "file",
		Help: "file api",
	}
	cmd.AddCommand(fileCommand)

	fileCommand.AddCommand(&grumble.Command{
		Name: "copy",
		Help: "copy file",
		Flags: func(f *grumble.Flags) {
			f.StringL("src", "", "src")
			f.StringL("dst", "", "dst")
			f.BoolL("meta", false, "meta")
		},
		Run: func(c *grumble.Context) error {
			return cli.FileCopy(c.Flags.String("src"), c.Flags.String("dst"), c.Flags.Bool("meta"))
		},
	})
	fileCommand.AddCommand(&grumble.Command{
		Name: "rename",
		Help: "rename file",
		Flags: func(f *grumble.Flags) {
			f.StringL("src", "", "src")
			f.StringL("dst", "", "dst")
		},
		Run: func(c *grumble.Context) error {
			return cli.FileRename(c.Flags.String("src"), c.Flags.String("dst"))
		},
	})
	fileCommand.AddCommand(&grumble.Command{
		Name: "delete",
		Help: "delete file or empty dir",
		Flags: func(f *grumble.Flags) {
			f.StringL("path", "", "path")
		},
		Run: func(c *grumble.Context) error {
			return cli.FileDelete(c.Flags.String("path"))
		},
	})
	fileCommand.AddCommand(&grumble.Command{
		Name: "upload",
		Help: "upload file",
		Flags: func(f *grumble.Flags) {
			f.StringL("path", "", "path")
			f.Uint64L("fileid", 0, "fileid")
			f.StringL("localpath", "", "localpath")
			f.StringL("raw", "", "raw")
		},
		Args: func(a *grumble.Args) {
			a.StringList("meta", "meta with key1 value1 key2 value2")
		},
		Run: func(c *grumble.Context) error {
			var body io.Reader
			if raw := c.Flags.String("raw"); len(raw) > 0 {
				body = bytes.NewReader([]byte(raw))
			} else {
				f, err := os.OpenFile(c.Flags.String("localpath"), os.O_RDONLY, 0o666)
				if err != nil {
					return err
				}
				defer f.Close()
				body = f
			}
			return show(cli.FileUpload(c.Flags.String("path"), c.Flags.Uint64("fileid"), body,
				c.Args.StringList("meta")...))
		},
	})
	fileCommand.AddCommand(&grumble.Command{
		Name: "write",
		Help: "write file",
		Flags: func(f *grumble.Flags) {
			f.StringL("path", "", "path")
			f.Uint64L("fileid", 0, "fileid")
			f.IntL("from", -1, "from")
			f.IntL("to", -1, "to")
			f.StringL("localpath", "", "localpath")
			f.StringL("raw", "", "raw")
		},
		Run: func(c *grumble.Context) error {
			var body io.Reader
			if raw := c.Flags.String("raw"); len(raw) > 0 {
				body = bytes.NewReader([]byte(raw))
			} else {
				f, err := os.OpenFile(c.Flags.String("localpath"), os.O_RDONLY, 0o666)
				if err != nil {
					return err
				}
				defer f.Close()
				body = f
			}
			from := c.Flags.Int("from")
			to := c.Flags.Int("to")
			if from < 0 || to < 0 {
				return fmt.Errorf("to set from(%d) and to(%d)", from, to)
			}
			return cli.FileWrite(c.Flags.String("path"), c.Flags.Uint64("fileid"), from, to, body)
		},
	})
	fileCommand.AddCommand(&grumble.Command{
		Name: "download",
		Help: "download file",
		Flags: func(f *grumble.Flags) {
			f.StringL("path", "", "path")
			f.IntL("from", -1, "from")
			f.IntL("to", -1, "to")
			f.StringL("localpath", "", "localpath")
		},
		Run: func(c *grumble.Context) error {
			r, err := cli.FileDownload(c.Flags.String("path"), c.Flags.Int("from"), c.Flags.Int("to"))
			if err != nil {
				return err
			}
			defer r.Close()

			localpath := c.Flags.String("localpath")
			if localpath != "" {
				f, err := os.OpenFile(localpath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666)
				if err != nil {
					return err
				}
				defer f.Close()
				io.Copy(f, r)
			} else {
				buff, err := io.ReadAll(r)
				if err != nil {
					return err
				}
				fmt.Println(string(buff))
			}
			return nil
		},
	})
}

func show(r interface{}, err error) error {
	if err != nil {
		return err
	}
	fmt.Println(common.Readable(r))
	return nil
}

func registerDrive(app *grumble.App) {
	driveCommand := &grumble.Command{
		Name: "drive",
		Help: "drive commands",
	}
	app.AddCommand(driveCommand)

	addCmdDrive(driveCommand)
	addCmdConfig(driveCommand)
	addCmdMeta(driveCommand)
	addCmdDir(driveCommand)
	addCmdFile(driveCommand)
}
