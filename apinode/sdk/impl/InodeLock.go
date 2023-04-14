package impl

type InodeLock struct{}

func (l *InodeLock) Lock(inode uint64, expireTime int) error {

	return nil
}

func (l *InodeLock) UnLock(inode uint64) error {
	return nil
}
