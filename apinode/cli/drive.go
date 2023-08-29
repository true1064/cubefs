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
	"encoding/json"
	"io"
	"os"

	"github.com/desertbit/grumble"

	"github.com/cubefs/cubefs/apinode/drive"
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
		Name: "del",
		Help: "delete meta",
		Args: func(a *grumble.Args) {
			a.String("path", "path name")
			a.StringList("meta", "meta with key1 key2 ...")
		},
		Run: func(c *grumble.Context) error {
			return cli.MetaDel(c.Args.String("path"), c.Args.StringList("meta")...)
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
			f.BoolL("recursive", false, "recursive")
		},
		Run: func(c *grumble.Context) error {
			return cli.FileDelete(c.Flags.String("path"), c.Flags.Bool("recursive"))
		},
	})
	fileCommand.AddCommand(&grumble.Command{
		Name: "batchdelete",
		Help: "batch delete files",
		Args: func(a *grumble.Args) {
			a.StringList("paths", "file paths")
		},
		Run: func(c *grumble.Context) error {
			return show(cli.FileBatchDelete(c.Args.StringList("paths")))
		},
	})
	fileCommand.AddCommand(&grumble.Command{
		Name: "verify",
		Help: "verify file content checksum",
		Args: func(a *grumble.Args) {
			a.StringList("checksum", "checksum key1 value1 key2 value2")
		},
		Flags: func(f *grumble.Flags) {
			f.StringL("path", "", "path")
			f.IntL("from", -1, "from")
			f.IntL("to", -1, "to")
		},
		Run: func(c *grumble.Context) error {
			checksum := make(map[string]string)
			list := c.Args.StringList("checksum")
			if len(list)%2 == 1 {
				list = list[:len(list)-1]
			}
			for ii := 0; ii < len(list); ii += 2 {
				checksum[list[ii]] = list[ii+1]
			}
			from := c.Flags.Int("from")
			to := c.Flags.Int("to")
			if from < 0 && to >= 0 {
				return fmt.Errorf("to set from(%d)", from)
			}
			return cli.FileVerify(c.Flags.String("path"), from, to, checksum)
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
			var size int
			if raw := c.Flags.String("raw"); len(raw) > 0 {
				body = bytes.NewReader([]byte(raw))
				size = len(raw)
			} else {
				st, err := os.Stat(c.Flags.String("localpath"))
				if err != nil {
					return err
				}
				size = int(st.Size())

				f, err := os.OpenFile(c.Flags.String("localpath"), os.O_RDONLY, 0o666)
				if err != nil {
					return err
				}
				defer f.Close()
				body = f
			}
			from := c.Flags.Int("from")
			to := c.Flags.Int("to")
			if from < 0 {
				return fmt.Errorf("to set from(%d) or to(%d)", from, to)
			}
			return cli.FileWrite(c.Flags.String("path"), c.Flags.Uint64("fileid"), from, to, body, size)
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
			var w io.Writer
			printSting := func() {}
			localpath := c.Flags.String("localpath")
			if localpath != "" {
				f, err := os.OpenFile(localpath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666)
				if err != nil {
					return err
				}
				defer f.Close()
				w = f
			} else {
				b := bytes.NewBuffer(nil)
				printSting = func() { fmt.Println(b.String()) }
				w = b
			}

			from := c.Flags.Int("from")
			to := c.Flags.Int("to")
			if from < 0 && to >= 0 {
				return fmt.Errorf("to set from(%d)", from)
			}
			err := cli.FileDownload(c.Flags.String("path"), from, to, w)
			printSting()
			return err
		},
	})
}

func addCmdMultipart(cmd *grumble.Command) {
	mpCommand := &grumble.Command{
		Name: "mp",
		Help: "multi part",
		Flags: func(f *grumble.Flags) {
			f.StringL("path", "", "path")
			f.StringL("fileid", "", "fileid")
			f.StringL("localpath", "", "localpath")
			f.IntL("partsize", 1<<20, "partsize")
		},
		Args: func(a *grumble.Args) {
			a.StringList("meta", "meta with key1 value1 key2 value2")
		},
		Run: func(c *grumble.Context) error {
			path := c.Flags.String("path")
			fd, err := os.OpenFile(c.Flags.String("localpath"), os.O_RDONLY, 0o666)
			if err != nil {
				return err
			}
			defer fd.Close()
			fmt.Println("to multipart:", path)

			mp, err := cli.MPInit(path, c.Flags.String("fileid"), c.Args.StringList("meta")...)
			if err != nil {
				return err
			}
			uploadID := mp.UploadID
			fmt.Println("   upload id:", uploadID)

			var abort bool
			defer func() {
				if abort {
					cli.MPAbort(path, uploadID)
				}
			}()

			parts := make([]drive.MPPart, 0)
			buffer := make([]byte, c.Flags.Int("partsize"))
			var partNumber, n int
			for {
				partNumber++
				n, err = io.ReadFull(fd, buffer)
				if n > 0 {
					var part drive.MPPart
					part, err = cli.MPPart(path, uploadID, fmt.Sprint(partNumber), bytes.NewReader(buffer[:n]))
					if err != nil {
						abort = true
						return err
					}
					fmt.Println(" upload part:", partNumber, part)
					parts = append(parts, part)
				}

				if err != nil {
					if err == io.EOF || err == io.ErrUnexpectedEOF {
						break
					}
					abort = true
					return err
				}
			}

			b, _ := json.Marshal(parts)
			r, err := cli.MPComplete(path, uploadID, bytes.NewReader(b))
			if err != nil {
				abort = true
				return err
			}
			return show(r, nil)
		},
	}
	cmd.AddCommand(mpCommand)

	mpCommand.AddCommand(&grumble.Command{
		Name: "init",
		Help: "multipart init",
		Flags: func(f *grumble.Flags) {
			flagsStrings(f, "path", "fileid")
		},
		Args: func(a *grumble.Args) {
			a.StringList("meta", "meta with key1 value1 key2 value2")
		},
		Run: func(c *grumble.Context) error {
			f := c.Flags.String
			return show(cli.MPInit(f("path"), f("fileid"), c.Args.StringList("meta")...))
		},
	})
	mpCommand.AddCommand(&grumble.Command{
		Name: "complete",
		Help: "multipart complete",
		Flags: func(f *grumble.Flags) {
			flagsStrings(f, "path", "uploadid", "localpath")
		},
		Run: func(c *grumble.Context) error {
			f := c.Flags.String
			fd, err := os.OpenFile(f("localpath"), os.O_RDONLY, 0o666)
			if err != nil {
				return err
			}
			defer fd.Close()
			return show(cli.MPComplete(f("path"), f("uploadid"), fd))
		},
	})
	mpCommand.AddCommand(&grumble.Command{
		Name: "part",
		Help: "multipart upload part",
		Flags: func(f *grumble.Flags) {
			flagsStrings(f, "path", "uploadid", "partnumber", "localpath")
		},
		Run: func(c *grumble.Context) error {
			f := c.Flags.String
			fd, err := os.OpenFile(f("localpath"), os.O_RDONLY, 0o666)
			if err != nil {
				return err
			}
			defer fd.Close()
			return show(cli.MPPart(f("path"), f("uploadid"), f("partnumber"), fd))
		},
	})
	mpCommand.AddCommand(&grumble.Command{
		Name: "list",
		Help: "multipart list parts",
		Flags: func(f *grumble.Flags) {
			flagsStrings(f, "path", "uploadid", "marker", "limit")
		},
		Run: func(c *grumble.Context) error {
			f := c.Flags.String
			return show(cli.MPList(f("path"), f("uploadid"), f("marker"), f("limit")))
		},
	})
	mpCommand.AddCommand(&grumble.Command{
		Name: "abort",
		Help: "multipart abort",
		Flags: func(f *grumble.Flags) {
			flagsStrings(f, "path", "uploadid")
		},
		Run: func(c *grumble.Context) error {
			f := c.Flags.String
			return cli.MPAbort(f("path"), f("uploadid"))
		},
	})
}

func flagsStrings(f *grumble.Flags, keys ...string) {
	for _, k := range keys {
		f.StringL(k, "", k)
	}
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
	addCmdMeta(driveCommand)
	addCmdDir(driveCommand)
	addCmdFile(driveCommand)
	addCmdMultipart(driveCommand)
}
