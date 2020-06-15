package fuse

import (
	"fmt"
	"time"

	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"

	"golang.org/x/net/context"
)

// SnapshotsDir is a fuse directory which contains snapshots named by timestamp.
type SnapshotsDir struct {
	inode   uint64
	root    *Root
	names   map[string]*restic.Snapshot
	latest  string
	tag     string
	host    string
	snCount int

	template string
}

// SnapshotsIDSDir is a fuse directory which contains snapshots named by ids.
type SnapshotsIDSDir struct {
	inode   uint64
	root    *Root
	names   map[string]*restic.Snapshot
	snCount int
}

// HostsDir is a fuse directory which contains hosts.
type HostsDir struct {
	inode   uint64
	root    *Root
	hosts   map[string]bool
	snCount int
}

// TagsDir is a fuse directory which contains tags.
type TagsDir struct {
	inode   uint64
	root    *Root
	tags    map[string]bool
	snCount int
}

// SnapshotLink
type snapshotLink struct {
	root     *Root
	inode    uint64
	target   string
	snapshot *restic.Snapshot
}

// read tag names from the current repository-state.
func updateTagNames(d *TagsDir) {
	if d.snCount != d.root.snCount {
		d.snCount = d.root.snCount
		d.tags = make(map[string]bool, len(d.root.snapshots))
		for _, snapshot := range d.root.snapshots {
			for _, tag := range snapshot.Tags {
				if tag != "" {
					d.tags[tag] = true
				}
			}
		}
	}
}

// read host names from the current repository-state.
func updateHostsNames(d *HostsDir) {
	if d.snCount != d.root.snCount {
		d.snCount = d.root.snCount
		d.hosts = make(map[string]bool, len(d.root.snapshots))
		for _, snapshot := range d.root.snapshots {
			d.hosts[snapshot.Hostname] = true
		}
	}
}

// read snapshot id names from the current repository-state.
func updateSnapshotIDSNames(d *SnapshotsIDSDir) {
	if d.snCount != d.root.snCount {
		d.snCount = d.root.snCount
		for _, sn := range d.root.snapshots {
			name := sn.ID().Str()
			d.names[name] = sn
		}
	}
}

// NewSnapshotsDir returns a new directory containing snapshots.
func NewSnapshotsDir(root *Root, inode uint64, tag string, host string) *SnapshotsDir {
	debug.Log("create snapshots dir, inode %d", inode)
	d := &SnapshotsDir{
		root:     root,
		inode:    inode,
		names:    make(map[string]*restic.Snapshot),
		latest:   "",
		tag:      tag,
		host:     host,
		template: root.cfg.SnapshotTemplate,
	}

	return d
}

// NewSnapshotsIDSDir returns a new directory containing snapshots named by ids.
func NewSnapshotsIDSDir(root *Root, inode uint64) *SnapshotsIDSDir {
	debug.Log("create snapshots ids dir, inode %d", inode)
	d := &SnapshotsIDSDir{
		root:  root,
		inode: inode,
		names: make(map[string]*restic.Snapshot),
	}

	return d
}

// NewHostsDir returns a new directory containing host names
func NewHostsDir(root *Root, inode uint64) *HostsDir {
	debug.Log("create hosts dir, inode %d", inode)
	d := &HostsDir{
		root:  root,
		inode: inode,
		hosts: make(map[string]bool),
	}

	return d
}

// NewTagsDir returns a new directory containing tag names
func NewTagsDir(root *Root, inode uint64) *TagsDir {
	debug.Log("create tags dir, inode %d", inode)
	d := &TagsDir{
		root:  root,
		inode: inode,
		tags:  make(map[string]bool),
	}

	return d
}

// newSnapshotLink
func newSnapshotLink(ctx context.Context, root *Root, inode uint64, target string, snapshot *restic.Snapshot) (*snapshotLink, error) {
	return &snapshotLink{root: root, inode: inode, target: target, snapshot: snapshot}, nil
}

// search element in string list.
func isElem(e string, list []string) bool {
	for _, x := range list {
		if e == x {
			return true
		}
	}
	return false
}

const minSnapshotsReloadTime = 60 * time.Second

// update snapshots if repository has changed
func updateSnapshots(ctx context.Context, root *Root) error {
	if time.Since(root.lastCheck) < minSnapshotsReloadTime {
		return nil
	}

	snapshots, err := restic.FindFilteredSnapshots(ctx, root.repo, root.cfg.Hosts, root.cfg.Tags, root.cfg.Paths)
	if err != nil {
		return err
	}

	if root.snCount != len(snapshots) {
		root.snCount = len(snapshots)
		root.repo.LoadIndex(ctx)
		root.snapshots = snapshots
	}
	root.lastCheck = time.Now()

	return nil
}

// read snapshot timestamps from the current repository-state.
func updateSnapshotNames(d *SnapshotsDir, template string) {
	if d.snCount != d.root.snCount {
		d.snCount = d.root.snCount
		var latestTime time.Time
		d.latest = ""
		d.names = make(map[string]*restic.Snapshot, len(d.root.snapshots))
		for _, sn := range d.root.snapshots {
			if d.tag == "" || isElem(d.tag, sn.Tags) {
				if d.host == "" || d.host == sn.Hostname {
					name := sn.Time.Format(template)
					if d.latest == "" || !sn.Time.Before(latestTime) {
						latestTime = sn.Time
						d.latest = name
					}
					for i := 1; ; i++ {
						if _, ok := d.names[name]; !ok {
							break
						}

						name = fmt.Sprintf("%s-%d", sn.Time.Format(template), i)
					}

					d.names[name] = sn
				}
			}
		}
	}
}
