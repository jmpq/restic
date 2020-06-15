package fuse

import (
	"github.com/restic/restic/internal/errors"
	"github.com/restic/restic/internal/restic"

	"github.com/restic/restic/internal/debug"

	"golang.org/x/net/context"
)

// The default block size to report in stat
const blockSize = 512

type file struct {
	root  *Root
	node  *restic.Node
	inode uint64

	sizes []int
	blobs [][]byte
}

func newFile(ctx context.Context, root *Root, inode uint64, node *restic.Node) (fusefile *file, err error) {
	debug.Log("create new file for %v with %d blobs", node.Name, len(node.Content))
	var bytes uint64
	sizes := make([]int, len(node.Content))
	for i, id := range node.Content {
		size, ok := root.blobSizeCache.Lookup(id)
		if !ok {
			var found bool
			size, found = root.repo.LookupBlobSize(id, restic.DataBlob)
			if !found {
				return nil, errors.Errorf("id %v not found in repository", id)
			}
		}

		sizes[i] = int(size)
		bytes += uint64(size)
	}

	if bytes != node.Size {
		debug.Log("sizes do not match: node.Size %v != size %v, using real size", node.Size, bytes)
		node.Size = bytes
	}

	return &file{
		inode: inode,
		root:  root,
		node:  node,
		sizes: sizes,
		blobs: make([][]byte, len(node.Content)),
	}, nil
}

func (f *file) getBlobAt(ctx context.Context, i int) (blob []byte, err error) {
	debug.Log("getBlobAt(%v, %v)", f.node.Name, i)
	if f.blobs[i] != nil {
		return f.blobs[i], nil
	}

	// release earlier blobs
	for j := 0; j < i; j++ {
		f.blobs[j] = nil
	}

	blob, err = f.root.repo.LoadBlob(ctx, restic.DataBlob, f.node.Content[i], nil)
	if err != nil {
		debug.Log("LoadBlob(%v, %v) failed: %v", f.node.Name, f.node.Content[i], err)
		return nil, err
	}
	f.blobs[i] = blob

	return blob, nil
}

func (f *file) readData(ctx context.Context, offset int64, data []byte) int {
	size := len(data)
	debug.Log("Read(%v, %v, %v), file size %v", f.node.Name, size, offset, f.node.Size)

	if uint64(offset) > f.node.Size {
		debug.Log("Read(%v): offset is greater than file size: %v > %v",
			f.node.Name, offset, f.node.Size)

		// return no data
		data = data[:0]
		return 0
	}

	// handle special case: file is empty
	if f.node.Size == 0 {
		data = data[:0]
		return 0
	}

	// Skip blobs before the offset
	startContent := 0
	for offset > int64(f.sizes[startContent]) {
		offset -= int64(f.sizes[startContent])
		startContent++
	}

	dst := data[0:size]
	readBytes := 0
	remainingBytes := size
	for i := startContent; remainingBytes > 0 && i < len(f.sizes); i++ {
		blob, err := f.getBlobAt(ctx, i)
		if err != nil {
			return readBytes
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
	data = data[:readBytes]

	return readBytes
}

func (f *file) release(ctx context.Context) error {
	for i := range f.blobs {
		f.blobs[i] = nil
	}
	return nil
}
