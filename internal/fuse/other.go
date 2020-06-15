package fuse

import (
	"github.com/restic/restic/internal/restic"
	"golang.org/x/net/context"
)

type other struct {
	root  *Root
	node  *restic.Node
	inode uint64
}

func newOther(ctx context.Context, root *Root, inode uint64, node *restic.Node) (*other, error) {
	return &other{root: root, inode: inode, node: node}, nil
}
