// +build darwin freebsd linux

package fuse

import (
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"
	"golang.org/x/net/context"
)

type Node fs.Node

// Statically ensure that *dir implement those interface
var _ = fs.HandleReadDirAller(&dir{})
var _ = fs.NodeStringLookuper(&dir{})

func (d *dir) Attr(ctx context.Context, a *fuse.Attr) error {
	debug.Log("Attr()")
	a.Inode = d.inode
	a.Mode = os.ModeDir | d.node.Mode
	a.Uid = d.root.uid
	a.Gid = d.root.gid
	a.Atime = d.node.AccessTime
	a.Ctime = d.node.ChangeTime
	a.Mtime = d.node.ModTime

	a.Nlink = d.calcNumberOfLinks()

	return nil
}

func (d *dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	debug.Log("ReadDirAll()")
	ret := make([]fuse.Dirent, 0, len(d.items)+2)

	ret = append(ret, fuse.Dirent{
		Inode: d.inode,
		Name:  ".",
		Type:  fuse.DT_Dir,
	})

	ret = append(ret, fuse.Dirent{
		Inode: d.parentInode,
		Name:  "..",
		Type:  fuse.DT_Dir,
	})

	for _, node := range d.items {
		name := cleanupNodeName(node.Name)
		var typ fuse.DirentType
		switch node.Type {
		case "dir":
			typ = fuse.DT_Dir
		case "file":
			typ = fuse.DT_File
		case "symlink":
			typ = fuse.DT_Link
		}

		ret = append(ret, fuse.Dirent{
			Inode: fs.GenerateDynamicInode(d.inode, name),
			Type:  typ,
			Name:  name,
		})
	}

	return ret, nil
}

func (d *dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	debug.Log("Lookup(%v)", name)
	node, ok := d.items[name]
	if !ok {
		debug.Log("  Lookup(%v) -> not found", name)
		return nil, fuse.ENOENT
	}
	switch node.Type {
	case "dir":
		return newDir(ctx, d.root, fs.GenerateDynamicInode(d.inode, name), d.inode, node)
	case "file":
		return newFile(ctx, d.root, fs.GenerateDynamicInode(d.inode, name), node)
	case "symlink":
		return newLink(ctx, d.root, fs.GenerateDynamicInode(d.inode, name), node)
	case "dev", "chardev", "fifo", "socket":
		return newOther(ctx, d.root, fs.GenerateDynamicInode(d.inode, name), node)
	default:
		debug.Log("  node %v has unknown type %v", name, node.Type)
		return nil, fuse.ENOENT
	}
}

func (d *dir) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	debug.Log("Listxattr(%v, %v)", d.node.Name, req.Size)
	for _, attr := range d.node.ExtendedAttributes {
		resp.Append(attr.Name)
	}
	return nil
}

func (d *dir) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	debug.Log("Getxattr(%v, %v, %v)", d.node.Name, req.Name, req.Size)
	attrval := d.node.GetExtendedAttribute(req.Name)
	if attrval != nil {
		resp.Xattr = attrval
		return nil
	}
	return fuse.ErrNoXattr
}

// Statically ensure that *file implements the given interface
var _ = fs.HandleReader(&file{})
var _ = fs.HandleReleaser(&file{})

func (f *file) Attr(ctx context.Context, a *fuse.Attr) error {
	debug.Log("Attr(%v)", f.node.Name)
	a.Inode = f.inode
	a.Mode = f.node.Mode
	a.Size = f.node.Size
	a.Blocks = (f.node.Size / blockSize) + 1
	a.BlockSize = blockSize
	a.Nlink = uint32(f.node.Links)

	if !f.root.cfg.OwnerIsRoot {
		a.Uid = f.node.UID
		a.Gid = f.node.GID
	}
	a.Atime = f.node.AccessTime
	a.Ctime = f.node.ChangeTime
	a.Mtime = f.node.ModTime

	return nil

}

func (f *file) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	debug.Log("Read(%v, %v, %v), file size %v", f.node.Name, req.Size, req.Offset, f.node.Size)
	offset := req.Offset

	if uint64(offset) > f.node.Size {
		debug.Log("Read(%v): offset is greater than file size: %v > %v",
			f.node.Name, req.Offset, f.node.Size)

		// return no data
		resp.Data = resp.Data[:0]
		return nil
	}

	// handle special case: file is empty
	if f.node.Size == 0 {
		resp.Data = resp.Data[:0]
		return nil
	}

	// Skip blobs before the offset
	startContent := 0
	for offset > int64(f.sizes[startContent]) {
		offset -= int64(f.sizes[startContent])
		startContent++
	}

	dst := resp.Data[0:req.Size]
	readBytes := 0
	remainingBytes := req.Size
	for i := startContent; remainingBytes > 0 && i < len(f.sizes); i++ {
		blob, err := f.getBlobAt(ctx, i)
		if err != nil {
			return err
		}

		if offset > 0 {
			blob = blob[offset:]
			offset = 0
		}

		copied := copy(dst, blob)
		remainingBytes -= copied
		readBytes += copied

		dst = dst[copied:]
	}
	resp.Data = resp.Data[:readBytes]

	return nil
}

func (f *file) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	for i := range f.blobs {
		f.blobs[i] = nil
	}
	return nil
}

func (f *file) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	debug.Log("Listxattr(%v, %v)", f.node.Name, req.Size)
	for _, attr := range f.node.ExtendedAttributes {
		resp.Append(attr.Name)
	}
	return nil
}

func (f *file) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	debug.Log("Getxattr(%v, %v, %v)", f.node.Name, req.Name, req.Size)
	attrval := f.node.GetExtendedAttribute(req.Name)
	if attrval != nil {
		resp.Xattr = attrval
		return nil
	}
	return fuse.ErrNoXattr
}

// Statically ensure that *link implements the given interface
var _ = fs.NodeReadlinker(&link{})

func (l *link) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	return l.node.LinkTarget, nil
}

func (l *link) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = l.inode
	a.Mode = l.node.Mode

	if !l.root.cfg.OwnerIsRoot {
		a.Uid = l.node.UID
		a.Gid = l.node.GID
	}
	a.Atime = l.node.AccessTime
	a.Ctime = l.node.ChangeTime
	a.Mtime = l.node.ModTime

	a.Nlink = uint32(l.node.Links)

	return nil
}

// ensure that *MetaDir implements these interfaces
var _ = fs.HandleReadDirAller(&MetaDir{})
var _ = fs.NodeStringLookuper(&MetaDir{})

// Attr returns the attributes for the root node.
func (d *MetaDir) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Inode = d.inode
	attr.Mode = os.ModeDir | 0555
	attr.Uid = d.root.uid
	attr.Gid = d.root.gid

	debug.Log("attr: %v", attr)
	return nil
}

// ReadDirAll returns all entries of the root node.
func (d *MetaDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	debug.Log("ReadDirAll()")
	items := []fuse.Dirent{
		{
			Inode: d.inode,
			Name:  ".",
			Type:  fuse.DT_Dir,
		},
		{
			Inode: d.root.inode,
			Name:  "..",
			Type:  fuse.DT_Dir,
		},
	}

	for name := range d.entries {
		items = append(items, fuse.Dirent{
			Inode: fs.GenerateDynamicInode(d.inode, name),
			Name:  name,
			Type:  fuse.DT_Dir,
		})
	}

	return items, nil
}

// Lookup returns a specific entry from the root node.
func (d *MetaDir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	debug.Log("Lookup(%s)", name)

	if dir, ok := d.entries[name]; ok {
		return dir, nil
	}

	return nil, fuse.ENOENT
}

// ensure that *other implements these interfaces
var _ = fs.NodeReadlinker(&other{})

func (l *other) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	return l.node.LinkTarget, nil
}

func (l *other) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = l.inode
	a.Mode = l.node.Mode

	if !l.root.cfg.OwnerIsRoot {
		a.Uid = l.node.UID
		a.Gid = l.node.GID
	}
	a.Atime = l.node.AccessTime
	a.Ctime = l.node.ChangeTime
	a.Mtime = l.node.ModTime

	a.Nlink = uint32(l.node.Links)

	return nil
}

// ensure that *SnapshotsDir implements these interfaces
var _ = fs.HandleReadDirAller(&SnapshotsDir{})
var _ = fs.NodeStringLookuper(&SnapshotsDir{})
var _ = fs.HandleReadDirAller(&SnapshotsIDSDir{})
var _ = fs.NodeStringLookuper(&SnapshotsIDSDir{})
var _ = fs.HandleReadDirAller(&TagsDir{})
var _ = fs.NodeStringLookuper(&TagsDir{})
var _ = fs.HandleReadDirAller(&HostsDir{})
var _ = fs.NodeStringLookuper(&HostsDir{})
var _ = fs.NodeReadlinker(&snapshotLink{})

// Attr returns the attributes for the root node.
func (d *SnapshotsDir) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Inode = d.inode
	attr.Mode = os.ModeDir | 0555
	attr.Uid = d.root.uid
	attr.Gid = d.root.gid

	debug.Log("attr: %v", attr)
	return nil
}

// Attr returns the attributes for the SnapshotsDir.
func (d *SnapshotsIDSDir) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Inode = d.inode
	attr.Mode = os.ModeDir | 0555
	attr.Uid = d.root.uid
	attr.Gid = d.root.gid

	debug.Log("attr: %v", attr)
	return nil
}

// Attr returns the attributes for the HostsDir.
func (d *HostsDir) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Inode = d.inode
	attr.Mode = os.ModeDir | 0555
	attr.Uid = d.root.uid
	attr.Gid = d.root.gid

	debug.Log("attr: %v", attr)
	return nil
}

// Attr returns the attributes for the TagsDir.
func (d *TagsDir) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Inode = d.inode
	attr.Mode = os.ModeDir | 0555
	attr.Uid = d.root.uid
	attr.Gid = d.root.gid

	debug.Log("attr: %v", attr)
	return nil
}

// ReadDirAll returns all entries of the SnapshotsDir.
func (d *SnapshotsDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	debug.Log("ReadDirAll()")

	// update snapshots
	updateSnapshots(ctx, d.root)

	// update snapshot names
	updateSnapshotNames(d, d.root.cfg.SnapshotTemplate)

	items := []fuse.Dirent{
		{
			Inode: d.inode,
			Name:  ".",
			Type:  fuse.DT_Dir,
		},
		{
			Inode: d.root.inode,
			Name:  "..",
			Type:  fuse.DT_Dir,
		},
	}

	for name := range d.names {
		items = append(items, fuse.Dirent{
			Inode: fs.GenerateDynamicInode(d.inode, name),
			Name:  name,
			Type:  fuse.DT_Dir,
		})
	}

	// Latest
	if d.latest != "" {
		items = append(items, fuse.Dirent{
			Inode: fs.GenerateDynamicInode(d.inode, "latest"),
			Name:  "latest",
			Type:  fuse.DT_Link,
		})
	}
	return items, nil
}

// ReadDirAll returns all entries of the SnapshotsIDSDir.
func (d *SnapshotsIDSDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	debug.Log("ReadDirAll()")

	// update snapshots
	updateSnapshots(ctx, d.root)

	// update snapshot ids
	updateSnapshotIDSNames(d)

	items := []fuse.Dirent{
		{
			Inode: d.inode,
			Name:  ".",
			Type:  fuse.DT_Dir,
		},
		{
			Inode: d.root.inode,
			Name:  "..",
			Type:  fuse.DT_Dir,
		},
	}

	for name := range d.names {
		items = append(items, fuse.Dirent{
			Inode: fs.GenerateDynamicInode(d.inode, name),
			Name:  name,
			Type:  fuse.DT_Dir,
		})
	}

	return items, nil
}

// ReadDirAll returns all entries of the HostsDir.
func (d *HostsDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	debug.Log("ReadDirAll()")

	// update snapshots
	updateSnapshots(ctx, d.root)

	// update host names
	updateHostsNames(d)

	items := []fuse.Dirent{
		{
			Inode: d.inode,
			Name:  ".",
			Type:  fuse.DT_Dir,
		},
		{
			Inode: d.root.inode,
			Name:  "..",
			Type:  fuse.DT_Dir,
		},
	}

	for host := range d.hosts {
		items = append(items, fuse.Dirent{
			Inode: fs.GenerateDynamicInode(d.inode, host),
			Name:  host,
			Type:  fuse.DT_Dir,
		})
	}

	return items, nil
}

// ReadDirAll returns all entries of the TagsDir.
func (d *TagsDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	debug.Log("ReadDirAll()")

	// update snapshots
	updateSnapshots(ctx, d.root)

	// update tag names
	updateTagNames(d)

	items := []fuse.Dirent{
		{
			Inode: d.inode,
			Name:  ".",
			Type:  fuse.DT_Dir,
		},
		{
			Inode: d.root.inode,
			Name:  "..",
			Type:  fuse.DT_Dir,
		},
	}

	for tag := range d.tags {
		items = append(items, fuse.Dirent{
			Inode: fs.GenerateDynamicInode(d.inode, tag),
			Name:  tag,
			Type:  fuse.DT_Dir,
		})
	}

	return items, nil
}

// Readlink
func (l *snapshotLink) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	return l.target, nil
}

// Attr
func (l *snapshotLink) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = l.inode
	a.Mode = os.ModeSymlink | 0777
	a.Uid = l.root.uid
	a.Gid = l.root.gid
	a.Atime = l.snapshot.Time
	a.Ctime = l.snapshot.Time
	a.Mtime = l.snapshot.Time

	a.Nlink = 1

	return nil
}

// Lookup returns a specific entry from the SnapshotsDir.
func (d *SnapshotsDir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	debug.Log("Lookup(%s)", name)

	sn, ok := d.names[name]
	if !ok {
		// could not find entry. Updating repository-state
		updateSnapshots(ctx, d.root)

		// update snapshot names
		updateSnapshotNames(d, d.root.cfg.SnapshotTemplate)

		sn, ok := d.names[name]
		if ok {
			return newDirFromSnapshot(ctx, d.root, fs.GenerateDynamicInode(d.inode, name), sn)
		}

		if name == "latest" && d.latest != "" {
			sn, ok := d.names[d.latest]

			// internal error
			if !ok {
				return nil, fuse.ENOENT
			}

			return newSnapshotLink(ctx, d.root, fs.GenerateDynamicInode(d.inode, name), d.latest, sn)
		}
		return nil, fuse.ENOENT
	}

	return newDirFromSnapshot(ctx, d.root, fs.GenerateDynamicInode(d.inode, name), sn)
}

// Lookup returns a specific entry from the SnapshotsIDSDir.
func (d *SnapshotsIDSDir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	debug.Log("Lookup(%s)", name)

	sn, ok := d.names[name]
	if !ok {
		// could not find entry. Updating repository-state
		updateSnapshots(ctx, d.root)

		// update snapshot ids
		updateSnapshotIDSNames(d)

		sn, ok := d.names[name]
		if ok {
			return newDirFromSnapshot(ctx, d.root, fs.GenerateDynamicInode(d.inode, name), sn)
		}

		return nil, fuse.ENOENT
	}

	return newDirFromSnapshot(ctx, d.root, fs.GenerateDynamicInode(d.inode, name), sn)
}

// Lookup returns a specific entry from the HostsDir.
func (d *HostsDir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	debug.Log("Lookup(%s)", name)

	_, ok := d.hosts[name]
	if !ok {
		// could not find entry. Updating repository-state
		updateSnapshots(ctx, d.root)

		// update host names
		updateHostsNames(d)

		_, ok := d.hosts[name]
		if ok {
			return NewSnapshotsDir(d.root, fs.GenerateDynamicInode(d.root.inode, name), "", name), nil
		}

		return nil, fuse.ENOENT
	}

	return NewSnapshotsDir(d.root, fs.GenerateDynamicInode(d.root.inode, name), "", name), nil
}

// Lookup returns a specific entry from the TagsDir.
func (d *TagsDir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	debug.Log("Lookup(%s)", name)

	_, ok := d.tags[name]
	if !ok {
		// could not find entry. Updating repository-state
		updateSnapshots(ctx, d.root)

		// update tag names
		updateTagNames(d)

		_, ok := d.tags[name]
		if ok {
			return NewSnapshotsDir(d.root, fs.GenerateDynamicInode(d.root.inode, name), name, ""), nil
		}

		return nil, fuse.ENOENT
	}

	return NewSnapshotsDir(d.root, fs.GenerateDynamicInode(d.root.inode, name), name, ""), nil
}

// ensure that *Root implements these interfaces
var _ = fs.HandleReadDirAller(&Root{})
var _ = fs.NodeStringLookuper(&Root{})

var GenerateDynamicInode = fs.GenerateDynamicInode

// Root is just there to satisfy fs.Root, it returns itself.
func (r *Root) Root() (fs.Node, error) {
	debug.Log("Root()")
	return r, nil
}
