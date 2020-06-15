package fuse

import (
	"github.com/restic/restic/internal/restic"
	"golang.org/x/net/context"
)

type link struct {
	root  *Root
	node  *restic.Node
	inode uint64
}

func newLink(ctx context.Context, root *Root, inode uint64, node *restic.Node) (*link, error) {
	return &link{root: root, inode: inode, node: node}, nil
}
