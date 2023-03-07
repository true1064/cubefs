package drive

type UserSpace struct {
	Uid         int    `json:"uid"`
	ClusterType int8   `json:"clusterType"`
	ClusterID   int    `json:"clusterId"`
	VolumeID    int    `json:"volumeId"`
	DriveID     int    `json:"driveId"`
	Capacity    uint64 `json:"capacity"`
	RootPath    string `json:"rootPath"`
	RootFileID  string `json:"rootFileId"`
	Ctime       uint64 `json:"ctime"`
	Params      string `json:"params"`
}

type PathInfo struct {
	Path   string `json:"path"`
	Status int8   `json:"status"`
	Mtime  uint64 `json:"mtime"`
}

type UserRoute struct {
	Uid      int        `json:"uid"`
	AppPaths []PathInfo `json:"appPaths"` //cloud path
}

type userRouteMgr struct {
	//TODO user route info store in lru cache
}

func (d *DriveNode) CreateUserSpace() {
	//1.Authenticate the token and get the uid

	//2.Applying for space to the cloud service

	//3.Apply to cfs for cluster and volume information

	//4.Locate the user file of the default cluster according to the hash of uid

	//5.Write mappings to extended attributes

	return
}

func (d *DriveNode) CreateRoute() {
	//1.Authenticate the token and get the uid

	//2.Get clusterid, volumeid from default cluster

	//3.Store user cloud directory

	return
}
