package fuse

import (
	"github.com/restic/restic/internal/debug"
)

// MetaDir is a fuse directory which contains other directories.
type MetaDir struct {
	inode   uint64
	root    *Root
	entries map[string]Node
}

// NewMetaDir returns a new meta dir.
func NewMetaDir(root *Root, inode uint64, entries map[string]Node) *MetaDir {
	debug.Log("new meta dir with %d entries, inode %d", len(entries), inode)

	return &MetaDir{
		root:    root,
		inode:   inode,
		entries: entries,
	}
}
