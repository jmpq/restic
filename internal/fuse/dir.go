package fuse

import (
	"os"
	"path/filepath"

	"golang.org/x/net/context"

	"github.com/restic/restic/internal/debug"
	"github.com/restic/restic/internal/restic"
)

type dir struct {
	root        *Root
	items       map[string]*restic.Node
	inode       uint64
	parentInode uint64
	node        *restic.Node

	blobsize *BlobSizeCache
}

func cleanupNodeName(name string) string {
	return filepath.Base(name)
}

func newDir(ctx context.Context, root *Root, inode, parentInode uint64, node *restic.Node) (*dir, error) {
	debug.Log("new dir for %v (%v)", node.Name, node.Subtree)
	tree, err := root.repo.LoadTree(ctx, *node.Subtree)
	if err != nil {
		debug.Log("  error loading tree %v: %v", node.Subtree, err)
		return nil, err
	}
	items := make(map[string]*restic.Node)
	for _, node := range tree.Nodes {
		items[cleanupNodeName(node.Name)] = node
	}

	return &dir{
		root:        root,
		node:        node,
		items:       items,
		inode:       inode,
		parentInode: parentInode,
	}, nil
}

// replaceSpecialNodes replaces nodes with name "." and "/" by their contents.
// Otherwise, the node is returned.
func replaceSpecialNodes(ctx context.Context, repo restic.Repository, node *restic.Node) ([]*restic.Node, error) {
	if node.Type != "dir" || node.Subtree == nil {
		return []*restic.Node{node}, nil
	}

	if node.Name != "." && node.Name != "/" {
		return []*restic.Node{node}, nil
	}

	tree, err := repo.LoadTree(ctx, *node.Subtree)
	if err != nil {
		return nil, err
	}

	return tree.Nodes, nil
}

func newDirFromSnapshot(ctx context.Context, root *Root, inode uint64, snapshot *restic.Snapshot) (*dir, error) {
	debug.Log("new dir for snapshot %v (%v)", snapshot.ID(), snapshot.Tree)
	tree, err := root.repo.LoadTree(ctx, *snapshot.Tree)
	if err != nil {
		debug.Log("  loadTree(%v) failed: %v", snapshot.ID(), err)
		return nil, err
	}
	items := make(map[string]*restic.Node)
	for _, n := range tree.Nodes {
		nodes, err := replaceSpecialNodes(ctx, root.repo, n)
		if err != nil {
			debug.Log("  replaceSpecialNodes(%v) failed: %v", n, err)
			return nil, err
		}

		for _, node := range nodes {
			items[cleanupNodeName(node.Name)] = node
		}
	}

	return &dir{
		root: root,
		node: &restic.Node{
			AccessTime: snapshot.Time,
			ModTime:    snapshot.Time,
			ChangeTime: snapshot.Time,
			Mode:       os.ModeDir | 0555,
		},
		items: items,
		inode: inode,
	}, nil
}

func (d *dir) calcNumberOfLinks() uint32 {
	// a directory d has 2 hardlinks + the number
	// of directories contained by d
	var count uint32
	count = 2
	for _, node := range d.items {
		if node.Type == "dir" {
			count++
		}
	}
	return count
}
