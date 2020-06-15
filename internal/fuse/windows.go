// +build windows

package fuse

import (
	"encoding/binary"
	//"fmt"
	"hash/fnv"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
	//"reflect"

	"github.com/billziss-gh/cgofuse/fuse"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/debug"
	"golang.org/x/net/context"
)

type Node interface {
	Attr(attr *fuse.Stat_t) int
}

type NodeStringLookuper interface {
	Lookup(name string) (Node, error)
}

type HandleReleaser interface {
	Release() int
}

type NodeReadlinker interface {
	Readlink() (string, int)
}

type HandleReadDirAller interface {
	Readdir(fill func(name string, stat *fuse.Stat_t, offset int64) bool, offset int64) int
}

type HandleReader interface {
	Read(offset int64, data []byte) int
}

type NodeGetxattrer interface {
	Getxattr(name string)  ([]byte, int)
}

type NodeListxattrer interface {
	Listxattr(fill func(name string) bool) int
}

func time2ts(t *time.Time, ts *fuse.Timespec) {
	ts.Sec = t.Unix()
	ts.Nsec = int64(t.Nanosecond())
}

func convertFileMode(m os.FileMode) uint32 {
	var mode uint32
	if m & os.ModeDir != 0 {
		mode |= fuse.S_IFDIR
	}
	if m & os.ModeSymlink != 0 {
		mode |= fuse.S_IFLNK
	}
	mode |= uint32(m&os.ModePerm)
	return mode
}


var _ = NodeStringLookuper(&dir{})

func (d *dir) Attr(a *fuse.Stat_t) int {
	debug.Log("Attr()")

	a.Ino = d.inode
	a.Mode = convertFileMode(os.ModeDir | d.node.Mode)
	a.Uid = d.root.uid
	a.Gid = d.root.gid
	time2ts(&d.node.AccessTime, &a.Atim)
	time2ts(&d.node.ChangeTime, &a.Ctim)
	time2ts(&d.node.ModTime, &a.Mtim)

	a.Nlink = d.calcNumberOfLinks()

	return 0
}

func (d *dir) Readdir(
	fill func(name string, stat *fuse.Stat_t, offset int64) bool,
	offset int64) (code int)  {
	debug.Log("Readdir()")

	fill(".", &fuse.Stat_t{}, 0)
	fill("..", &fuse.Stat_t{}, 0)

	for _, node := range d.items {
		name := cleanupNodeName(node.Name)
		node, err := d.Lookup(name)
		if err != nil {
			continue
		}

		var stat fuse.Stat_t
		node.Attr(&stat)

		fill(name, &stat, 0)
	}

	return 0
}

func (d *dir) Lookup(name string) (Node, error) {
	debug.Log("Lookup(%v)", name)

	node, ok := d.items[name]
	if !ok {
		debug.Log("  Lookup(%v) -> not found", name)
		return nil, syscall.ENOENT
	}

	ctx := context.Background()
	switch node.Type {
	case "dir":
		return newDir(ctx, d.root, GenerateDynamicInode(d.inode, name), d.inode, node)
	case "file":
		return newFile(ctx, d.root, GenerateDynamicInode(d.inode, name), node)
	case "symlink":
		return newLink(ctx, d.root, GenerateDynamicInode(d.inode, name), node)
	case "dev", "chardev", "fifo", "socket":
		return newOther(ctx, d.root, GenerateDynamicInode(d.inode, name), node)
	default:
		debug.Log("  node %v has unknown type %v", name, node.Type)
		return nil, syscall.ENOENT
	}
}

func (d *dir) Listxattr(fill func(name string) bool) int {
	debug.Log("Listxattr(%v)", d.node.Name)
	for _, attr := range d.node.ExtendedAttributes {
		b := fill(attr.Name)
		if !b {
			return fuse.ERANGE
		}
	}
	return 0
}

func (d *dir) Getxattr(name string) ([]byte, int) {
	debug.Log("Getxattr(%v, %v)", d.node.Name, name)
	attrval := d.node.GetExtendedAttribute(name)
	if attrval != nil {
		return attrval, 0
	}
	return nil, fuse.ENODATA
}

// Statically ensure that *file implements the given interface
var _ = HandleReader(&file{})
var _ = HandleReleaser(&file{})

func (f *file) Attr(a *fuse.Stat_t) int {
	debug.Log("Attr(%v)", f.node.Name)

	a.Ino = f.inode
	a.Mode = convertFileMode(f.node.Mode)
	a.Size = int64(f.node.Size)
	a.Blocks = int64(f.node.Size / blockSize) + 1
	a.Blksize = blockSize
	a.Nlink = uint32(f.node.Links)

	if !f.root.cfg.OwnerIsRoot {
		a.Uid = f.node.UID
		a.Gid = f.node.GID
	}
	time2ts(&f.node.AccessTime, &a.Atim)
	time2ts(&f.node.ChangeTime, &a.Atim)
	time2ts(&f.node.ModTime, &a.Mtim)

	return 0
}

func (f *file) Release() int {
	err := f.release(context.Background())
	if err != nil {
		return fuse.EINVAL
	}
	return 0
}

func (f *file) Read(offset int64, data []byte) int {
	return f.readData(context.Background(), offset, data)
}

// Statically ensure that *link implements the given interface
var _ = NodeReadlinker(&link{})

func (l *link) Readlink() (string, int) {
	return l.node.LinkTarget, 0
}

func (l *link) Attr(a *fuse.Stat_t) int {
	a.Ino = l.inode
	a.Mode = convertFileMode(l.node.Mode)

	if !l.root.cfg.OwnerIsRoot {
		a.Uid = l.node.UID
		a.Gid = l.node.GID
	}

	time2ts(&l.node.AccessTime, &a.Atim)
	time2ts(&l.node.ChangeTime, &a.Atim)
	time2ts(&l.node.ModTime, &a.Mtim)

	a.Nlink = uint32(l.node.Links)

	return 0
}

// ensure that *MetaDir implements these interfaces
var _ = HandleReadDirAller(&MetaDir{})
var _ = NodeStringLookuper(&MetaDir{})

// Attr returns the attributes for the root node.
func (d *MetaDir) Attr(attr *fuse.Stat_t) int {
	attr.Ino = d.inode
	attr.Mode = convertFileMode(os.ModeDir | 0555)
	//attr.Mode = convertFileMode(os.ModeDir) | 0777
	attr.Uid = d.root.uid
	attr.Gid = d.root.gid

	debug.Log("attr: %v", attr)
	return 0
}

// Readdir returns all entries of the root node.
func (d *MetaDir) Readdir(
	fill func(name string, st *fuse.Stat_t, offset int64) bool,
	offset int64) int {
	debug.Log("Readdir()")

	fill(".", &fuse.Stat_t{Ino: d.inode, Mode: fuse.S_IFDIR}, 0)
	fill("..", &fuse.Stat_t{Ino: d.root.inode, Mode: fuse.S_IFDIR}, 0)

	for name := range d.entries {
		fill(name, 
			&fuse.Stat_t{
				Ino: GenerateDynamicInode(d.inode, name),
				Mode: fuse.S_IFDIR, 
				Gid: d.root.gid, 
				Uid: d.root.uid},
			 0)
	}

	return 0
}

// Lookup returns a specific entry from the root node.
func (d *MetaDir) Lookup(name string) (Node, error) {
	debug.Log("Lookup(%s)", name)

	if dir, ok := d.entries[name]; ok {
		return dir, nil
	}

	return nil, syscall.ENOENT
}

// ensure that *other implements these interfaces
var _ = NodeReadlinker(&other{})

func (l *other) Readlink() (string, int) {
	return l.node.LinkTarget, 0
}

func (l *other) Attr(a *fuse.Stat_t) int{
	a.Mode = convertFileMode(l.node.Mode)

	if !l.root.cfg.OwnerIsRoot {
		a.Uid = l.node.UID
		a.Gid = l.node.GID
	}

	time2ts(&l.node.AccessTime, &a.Atim)
	time2ts(&l.node.ChangeTime, &a.Atim)
	time2ts(&l.node.ModTime, &a.Mtim)

	a.Nlink = uint32(l.node.Links)

	return 0
}

// ensure that *SnapshotsDir implements these interfaces
var _ = HandleReadDirAller(&SnapshotsDir{})
var _ = NodeStringLookuper(&SnapshotsDir{})
var _ = HandleReadDirAller(&SnapshotsIDSDir{})
var _ = NodeStringLookuper(&SnapshotsIDSDir{})
var _ = HandleReadDirAller(&TagsDir{})
var _ = NodeStringLookuper(&TagsDir{})
var _ = HandleReadDirAller(&HostsDir{})
var _ = NodeStringLookuper(&HostsDir{})
var _ = NodeReadlinker(&snapshotLink{})


// Attr returns the attributes for the root node.
func (d *SnapshotsDir) Attr(attr *fuse.Stat_t) int {
	attr.Ino = d.inode
	attr.Mode = convertFileMode(os.ModeDir | 0555)
	attr.Uid = d.root.uid
	attr.Gid = d.root.gid

	debug.Log("attr: %v", attr)
	return 0
}

// Attr returns the attributes for the SnapshotsDir.
func (d *SnapshotsIDSDir) Attr(attr *fuse.Stat_t) int {
	attr.Ino = d.inode
	attr.Mode = convertFileMode(os.ModeDir | 0555)
	attr.Uid = d.root.uid
	attr.Gid = d.root.gid

	debug.Log("attr: %v", attr)
	return 0
}

// Attr returns the attributes for the HostsDir.
func (d *HostsDir) Attr(attr *fuse.Stat_t) int {
	attr.Ino = d.inode
	attr.Mode = convertFileMode(os.ModeDir | 0555)
	attr.Uid = d.root.uid
	attr.Gid = d.root.gid

	debug.Log("attr: %v", attr)
	return 0
}

// Attr returns the attributes for the TagsDir.
func (d *TagsDir) Attr(attr *fuse.Stat_t) int {
	attr.Ino = d.inode
	attr.Mode = convertFileMode(os.ModeDir | 0555)
	attr.Uid = d.root.uid
	attr.Gid = d.root.gid

	debug.Log("attr: %v", attr)
	return 0
}

// ReadDirAll returns all entries of the SnapshotsDir.
func (d *SnapshotsDir) Readdir(
	fill func(name string, st *fuse.Stat_t, offset int64) bool, 
	offset int64) int {
	debug.Log("Readdir()")

	ctx := context.Background()

	// update snapshots
	updateSnapshots(ctx, d.root)

	// update snapshot names
	updateSnapshotNames(d, d.root.cfg.SnapshotTemplate)

	fill(".", &fuse.Stat_t{Mode: syscall.S_IFDIR}, 0)
	fill("..", &fuse.Stat_t{Mode: syscall.S_IFDIR}, 0)

	for name := range d.names {
		fill(name, &fuse.Stat_t{Mode: syscall.S_IFDIR}, 0)
	}

	/*
	// Latest
	if d.latest != "" {
		fill("latest", &fuse.Stat_t{Mode: syscall.S_IFLNK}, 0)
	}
	*/
	return 0
}

// ReadDirAll returns all entries of the SnapshotsIDSDir.
func (d *SnapshotsIDSDir) Readdir(
	fill func(name string, st *fuse.Stat_t, offset int64) bool, 
	offset int64) int {
	debug.Log("Readdir()")

	ctx := context.Background()

	// update snapshots
	updateSnapshots(ctx, d.root)

	// update snapshot ids
	updateSnapshotIDSNames(d)

	fill(".", &fuse.Stat_t{Mode: syscall.S_IFDIR}, 0)
	fill("..", &fuse.Stat_t{Mode: syscall.S_IFDIR}, 0)

	for name := range d.names {
		fill(name, &fuse.Stat_t{Mode: syscall.S_IFDIR}, 0)
	}

	return 0
}

// ReadDirAll returns all entries of the HostsDir.
func (d *HostsDir) Readdir(
	fill func(name string, st *fuse.Stat_t, offset int64) bool, 
	offset int64) int {
	debug.Log("Readdir()")

	ctx := context.Background()

	// update snapshots
	updateSnapshots(ctx, d.root)

	// update host names
	updateHostsNames(d)

	fill(".", &fuse.Stat_t{Mode:syscall.S_IFDIR},  0)
	fill("..", &fuse.Stat_t{Mode:syscall.S_IFDIR},  0)

	for host := range d.hosts {
		fill(host, &fuse.Stat_t{Mode:syscall.S_IFDIR}, 0)
	}

	return 0 
}

// Readdir returns all entries of the TagsDir.
func (d *TagsDir) Readdir(
	fill func(name string, st *fuse.Stat_t, offset int64) bool, 
	offset int64) int {
	debug.Log("Readdir()")

	ctx := context.Background()

	// update snapshots
	updateSnapshots(ctx, d.root)

	// update tag names
	updateTagNames(d)

	fill(".", &fuse.Stat_t{Mode: syscall.S_IFDIR}, 0)
	fill("..", &fuse.Stat_t{Mode: syscall.S_IFDIR}, 0)

	for tag := range d.tags {
		fill(tag, &fuse.Stat_t{Mode: syscall.S_IFDIR}, 0)
	}

	return 0 
}

// Readlink
func (l *snapshotLink) Readlink() (string, int) {
	return l.target, 0
}

// Attr
func (l *snapshotLink) Attr(a *fuse.Stat_t) int {
	a.Mode = convertFileMode(os.ModeSymlink | 0777)
	a.Uid = l.root.uid
	a.Gid = l.root.gid

	time2ts(&l.snapshot.Time, &a.Atim)
	time2ts(&l.snapshot.Time, &a.Atim)
	time2ts(&l.snapshot.Time, &a.Mtim)

	a.Nlink = 1

	return 0
}

// Lookup returns a specific entry from the SnapshotsDir.
func (d *SnapshotsDir) Lookup(name string) (Node, error) {
	debug.Log("Lookup(%s)", name)

	ctx := context.Background()
	sn, ok := d.names[name]
	if !ok {
		// could not find entry. Updating repository-state
		updateSnapshots(ctx, d.root)

		// update snapshot names
		updateSnapshotNames(d, d.root.cfg.SnapshotTemplate)

		sn, ok := d.names[name]
		if ok {
			return newDirFromSnapshot(ctx, d.root, GenerateDynamicInode(d.inode, name), sn)
		}

		if name == "latest" && d.latest != "" {
			sn, ok := d.names[d.latest]

			// internal error
			if !ok {
				return nil, syscall.ENOENT
			}

			return newSnapshotLink(ctx, d.root, GenerateDynamicInode(d.inode, name), d.latest, sn)
		}
		return nil, syscall.ENOENT
	}

	return newDirFromSnapshot(ctx, d.root, GenerateDynamicInode(d.inode, name), sn)
}

// Lookup returns a specific entry from the SnapshotsIDSDir.
func (d *SnapshotsIDSDir) Lookup( name string) (Node, error) {
	debug.Log("Lookup(%s)", name)

	ctx:=context.Background()
	sn, ok := d.names[name]
	if !ok {
		// could not find entry. Updating repository-state
		updateSnapshots(ctx, d.root)

		// update snapshot ids
		updateSnapshotIDSNames(d)

		sn, ok := d.names[name]
		if ok {
			return newDirFromSnapshot(ctx, d.root, GenerateDynamicInode(d.inode, name), sn)
		}

		return nil, syscall.ENOENT
	}

	return newDirFromSnapshot(ctx, d.root, GenerateDynamicInode(d.inode, name), sn)
}

// Lookup returns a specific entry from the HostsDir.
func (d *HostsDir) Lookup(name string) (Node, error) {
	debug.Log("Lookup(%s)", name)

	ctx:=context.Background()
	_, ok := d.hosts[name]
	if !ok {
		// could not find entry. Updating repository-state
		updateSnapshots(ctx, d.root)

		// update host names
		updateHostsNames(d)

		_, ok := d.hosts[name]
		if ok {
			return NewSnapshotsDir(d.root, GenerateDynamicInode(d.root.inode, name), "", name), nil
		}

		return nil, syscall.ENOENT
	}

	return NewSnapshotsDir(d.root, GenerateDynamicInode(d.root.inode, name), "", name), nil
}

// Lookup returns a specific entry from the TagsDir.
func (d *TagsDir) Lookup(name string) (Node, error) {
	debug.Log("Lookup(%s)", name)

	ctx:=context.Background()
	_, ok := d.tags[name]
	if !ok {
		// could not find entry. Updating repository-state
		updateSnapshots(ctx, d.root)

		// update tag names
		updateTagNames(d)

		_, ok := d.tags[name]
		if ok {
			return NewSnapshotsDir(d.root, GenerateDynamicInode(d.root.inode, name), name, ""), nil
		}

		return nil, syscall.ENOENT
	}

	return NewSnapshotsDir(d.root, GenerateDynamicInode(d.root.inode, name), name, ""), nil
}


// ensure that *Root implements these interfaces
var _ = HandleReadDirAller(&Root{})
var _ = NodeStringLookuper(&Root{})

// GenerateDynamicInode returns a dynamic inode.
//
// The parent inode and current entry name are used as the criteria
// for choosing a pseudorandom inode. This makes it likely the same
// entry will get the same inode on multiple runs.
func GenerateDynamicInode(parent uint64, name string) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], parent)
	_, _ = h.Write(buf[:])
	_, _ = h.Write([]byte(name))
	var inode uint64
	for {
		inode = h.Sum64()
		if inode > 1 {
			break
		}
		// there's a tiny probability that result is zero or the
		// hardcoded root inode 1; change the input a little and try
		// again
		_, _ = h.Write([]byte{'x'})
	}
	return inode
}

type WinFs struct {
	fuse.FileSystemBase
	lock sync.Mutex
	root *Root
}

func (winfs *WinFs) Readlink(path string) (code int, target string) {
	_, _, node := winfs.lookupNode(path, nil)
	if nil == node {
		return -fuse.ENOENT, ""
	}

	var attr fuse.Stat_t
	code = node.Attr(&attr)
	if code != 0 {
		return code, ""
	}

	if fuse.S_IFLNK != attr.Mode&fuse.S_IFMT {
		return -fuse.EINVAL, ""
	}

	reader := node.(NodeReadlinker)
	if reader == nil {
		return -fuse.EINVAL, ""
	}
	target, code = reader.Readlink()

	return code, target
}

func (winfs *WinFs) Open(path string, flags int) (code int, fh uint64) {
	return 0, 0
}

func (winfs *WinFs) Getattr(path string, stat *fuse.Stat_t, fh uint64) (code int) {
	_, _, node := winfs.lookupNode(path, nil)
	if nil == node {
		return -fuse.ENOENT
	}

	if stat != nil {
		return node.Attr(stat)
	}

	return 0
}

func (winfs *WinFs) Read(path string, buff []byte, offset int64, fh uint64) (n int) {
	_, _, node := winfs.lookupNode(path, nil)
	if nil == node {
		return -fuse.ENOENT
	}

	reader := node.(HandleReader)
	if reader == nil {
		return -fuse.EINVAL
	}

	n = reader.Read(offset, buff)

	return
}

func (winfs *WinFs) Release(path string, fh uint64) (code int) {
	return 0
}

func (winfs *WinFs) Opendir(path string) (code int, fh uint64) {
	return 0, 1 
}

func (winfs *WinFs) Readdir(path string,
	fill func(name string, stat *fuse.Stat_t, offset int64) bool,
	offset int64,
	fh uint64) (code int) {

	_, _, node := winfs.lookupNode(path, nil)
	if node == nil {
		return -fuse.ENOENT
	}

	reader := node.(HandleReadDirAller)
	if reader == nil {
		return -fuse.EINVAL
	}

	code = reader.Readdir(fill, offset)
	return
}

func (winfs *WinFs) Releasedir(path string, fh uint64) (code int) {
	return 0
}

func (winfs *WinFs) Getxattr(path string, name string) (code int, xattr []byte) {
	_, _, node := winfs.lookupNode(path, nil)
	if nil == node {
		return -fuse.ENOENT, nil
	}

	getter := node.(NodeGetxattrer)
	if getter == nil {
		return -fuse.EINVAL, nil
	}


	xattr, code = getter.Getxattr(name)
	return code, xattr 
}

func (winfs *WinFs) Listxattr(path string, fill func(name string) bool) (code int) {
	_, _, node := winfs.lookupNode(path, nil)
	if nil == node {
		return -fuse.ENOENT
	}

	lister := node.(NodeListxattrer)
	if lister == nil {
		return -fuse.EINVAL
	}

	return lister.Listxattr(fill)
}

func (winfs *WinFs) lookupNode(path string, ancestor Node) (parent Node, name string, node Node) {
	parent = winfs.root
	name = ""
	node = winfs.root
	var err error

	for _, c := range strings.Split(path, "/") {
		if "" != c {
			if 255 < len(c) {
				panic(fuse.Error(-fuse.ENAMETOOLONG))
			}
			parent, name = node, c
			if node == nil {
				return
			}

			lookup := parent.(NodeStringLookuper)
			if lookup == nil {
				name = "" // parent is not a directory
				return
			}

			node, err = lookup.Lookup(c)
			if err != nil {
				name = "" // not found
				return
			}

			if nil != ancestor && node == ancestor {
				name = "" // special case loop condition
				return
			}
		}
	}
	return
}

func NewWinFs(ctx context.Context, repo restic.Repository, cfg Config) (*WinFs, error) {
	root, err := NewRoot(ctx, repo, cfg)
	if err != nil {
		return nil, err
	}
	fs := WinFs{root: root}
	return &fs, nil
}
