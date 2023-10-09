package metanode

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func initFsmDentryMp() *metaPartition {
	return &metaPartition{
		dentryTree: NewBtree(),
		inodeTree:  NewBtree(),
	}
}

func Test_fsmCreateDentryEx(t *testing.T) {
	req := &DentryEx{
		Dentry: &Dentry{
			ParentId:  100,
			Name:      "test",
			Inode:     101,
			Type:      0x001,
			FileId:    102,
			multiSnap: nil,
		},
		OldIno: 1001,
	}

	mp := initFsmDentryMp()
	status := mp.fsmCreateDentryEx(req)
	require.Equal(t, proto.OpNotExistErr, status)

	parIno := &Inode{
		Inode: req.ParentId,
		Type:  0,
	}

	mp.inodeTree.ReplaceOrInsert(parIno, false)

	parIno.Flag = DeleteMarkFlag
	status = mp.fsmCreateDentryEx(req)
	require.Equal(t, proto.OpNotExistErr, status)
	parIno.Flag = 0

	status = mp.fsmCreateDentryEx(req)
	require.Equal(t, proto.OpArgMismatchErr, status)
	parIno.Type = uint32(os.ModeDir)

	oldDen := &Dentry{
		ParentId: req.ParentId,
	}
	mp.dentryTree.ReplaceOrInsert(oldDen, false)

	status = mp.fsmCreateDentryEx(req)
	require.Equal(t, proto.OpNotExistErr, status)
	oldDen.Name = req.Name

	status = mp.fsmCreateDentryEx(req)
	require.Equal(t, proto.OpArgMismatchErr, status)

	oldDen.Inode = req.Inode
	status = mp.fsmCreateDentryEx(req)
	require.Equal(t, proto.OpOk, status)
	require.True(t, oldDen.FileId == 0)

	oldDen.Inode = req.OldIno
	status = mp.fsmCreateDentryEx(req)
	require.Equal(t, proto.OpOk, status)
	require.True(t, req.Type == oldDen.Type && req.Inode == oldDen.Inode && req.FileId == oldDen.FileId)
	require.True(t, oldDen.getSnapListLen() == 0)

	// version op
	req.multiSnap = &DentryMultiSnap{
		VerSeq: 10,
	}
	oldDen.Inode = req.OldIno
	status = mp.fsmCreateDentryEx(req)
	require.Equal(t, proto.OpOk, status)
	require.True(t, req.Type == oldDen.Type && req.Inode == oldDen.Inode && req.FileId == oldDen.FileId)
	require.True(t, oldDen.getSnapListLen() == 1)

	oldDen.Inode = req.OldIno
	oldDen.multiSnap.VerSeq = 9
	status = mp.fsmCreateDentryEx(req)
	require.Equal(t, proto.OpOk, status)
	require.True(t, req.Type == oldDen.Type && req.Inode == oldDen.Inode && req.FileId == oldDen.FileId)
	require.True(t, oldDen.getSnapListLen() == 2)

	oldDen.setDeleted()
	status = mp.fsmCreateDentryEx(req)
	require.Equal(t, proto.OpNotExistErr, status)
}
