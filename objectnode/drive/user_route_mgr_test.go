package drive

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestHash(t *testing.T) {
	//testNum must be an integer multiple of (hash_bucket_num*hash_bucket_num)
	testNum := 1000
	as := make(map[int]map[int]int)
	for i := 1; i <= testNum; i++ {
		l1, l2 := hash(i)
		if entry, ok := as[l1]; ok {
			if _, ok := entry[l2]; ok {
				entry[l2] += 1
				continue
			}
			entry[l2] = 1
			continue
		}
		as[l1] = make(map[int]int)
		as[l1][l2] = 1
	}
	l1Nums := make([]int, 0)
	l2Nums := make([]int, 0)
	for l1, l2map := range as {
		l1Nums = append(l1Nums, l1)
		for l2, count := range l2map {
			if l1 == 0 {
				l2Nums = append(l2Nums, l2)
			}
			require.Equal(t, testNum/(hashBucketNum*hashBucketNum), count)
		}
	}
	require.Equal(t, hashBucketNum, len(l1Nums))
	require.Equal(t, hashBucketNum, len(l2Nums))
}

func TestDriveNode_CreateUserSpace(t *testing.T) {
	dn := DriveNode{}
	dn.CreateUserRoute(178)
}
