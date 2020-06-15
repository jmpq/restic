package fuse

import (
	"os"
	"time"

	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"

	"golang.org/x/net/context"
)

// Config holds settings for the fuse mount.
type Config struct {
	OwnerIsRoot      bool
	Hosts            []string
	Tags             []restic.TagList
	Paths            []string
	SnapshotTemplate string
}

// Root is the root node of the fuse mount of a repository.
type Root struct {
	repo          restic.Repository
	cfg           Config
	inode         uint64
	snapshots     restic.Snapshots
	blobSizeCache *BlobSizeCache

	snCount   int
	lastCheck time.Time

	*MetaDir

	uid, gid uint32
}

const rootInode = 1

// NewRoot initializes a new root node from a repository.
func NewRoot(ctx context.Context, repo restic.Repository, cfg Config) (*Root, error) {
	debug.Log("NewRoot(), config %v", cfg)

	root := &Root{
		repo:          repo,
		inode:         rootInode,
		cfg:           cfg,
		blobSizeCache: NewBlobSizeCache(ctx, repo.Index()),
	}

	if !cfg.OwnerIsRoot {
		root.uid = uint32(os.Getuid())
		root.gid = uint32(os.Getgid())
	}

	entries := map[string]Node{
		"snapshots": NewSnapshotsDir(root, GenerateDynamicInode(root.inode, "snapshots"), "", ""),
		"tags":      NewTagsDir(root, GenerateDynamicInode(root.inode, "tags")),
		"hosts":     NewHostsDir(root, GenerateDynamicInode(root.inode, "hosts")),
		"ids":       NewSnapshotsIDSDir(root, GenerateDynamicInode(root.inode, "ids")),
	}

	root.MetaDir = NewMetaDir(root, rootInode, entries)

	return root, nil
}
