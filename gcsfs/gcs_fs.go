package gcsfs

import (
	"context"
	"fmt"
	"os"
	"sync"
	"io"
	"io/ioutil"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"cloud.google.com/go/storage"
)

/**
 * This filesystem only contains one file - gcs_file. We use this file
 * as an interface to transfer data between GCS object
 */
func NewGCSFileSystem(obj *storage.ObjectHandle) (fuse.Server, error) {
	fs := &gcsFS{obj: obj}
	return fuseutil.NewFileSystemServer(fs), nil
}

const (
	gcsFileInodeID = fuseops.RootInodeID + 1 + iota
	barID
)

type gcsFS struct {
	fuseutil.NotImplementedFileSystem

	mu     sync.Mutex
	obj    *storage.ObjectHandle
	writer *storage.Writer
}

const gcsFileName = "gcs_file"

////////////////////////////////////////////////////////////////////////
// Helper methods
////////////////////////////////////////////////////////////////////////

func (fs *gcsFS) gcsFileAttributes() fuseops.InodeAttributes {
	ctx := context.Background()
	var size uint64 = 0
	objAttrs, err := fs.obj.Attrs(ctx)
	if err != nil {
		fmt.Println("Failed to get attrs of object")
	} else {
		// load the file size from GCS object metadata
		size = uint64(objAttrs.Size)
	}
	return fuseops.InodeAttributes{
		Nlink: 1,
		Mode: 0777,
		Size: size,
	}
}

////////////////////////////////////////////////////////////////////////
// File system methods
////////////////////////////////////////////////////////////////////////

func (fs *gcsFS) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	switch {
	case op.Inode == fuseops.RootInodeID:
		op.Attributes = fuseops.InodeAttributes{
			Nlink: 1,
			Mode:  0777 | os.ModeDir,
		}

	case op.Inode == gcsFileInodeID:
		op.Attributes = fs.gcsFileAttributes()

	default:
		return fmt.Errorf("Unknown inode: %d", op.Inode)
	}

	return nil
}

func (fs *gcsFS) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) error {
	fmt.Println("StatFS")
	return nil
}

func (fs *gcsFS) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if op.Parent != fuseops.RootInodeID {
		return fuse.ENOENT
	}

	switch op.Name {
	case gcsFileName:
		op.Entry = fuseops.ChildInodeEntry{
			Child: gcsFileInodeID,
			Attributes: fs.gcsFileAttributes(),
		}

	default:
		return fuse.ENOENT
	}

	return nil
}

func (fs *gcsFS) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fmt.Println("OpenDir")

	if op.Inode != fuseops.RootInodeID {
		return fmt.Errorf("Unsupported inode ID: %d", op.Inode)
	}

	return nil
}

func (fs *gcsFS) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fmt.Println("ReadDir")

	var dirents []fuseutil.Dirent

	switch op.Inode {
	case fuseops.RootInodeID:
		dirents = []fuseutil.Dirent{
			fuseutil.Dirent{
				Offset: 1,
				Inode: gcsFileInodeID,
				Name: gcsFileName,
				Type: fuseutil.DT_File,
			},
		}

	default:
		return fmt.Errorf("Unexpected inode: %v", op.Inode)
	}

	switch op.Offset {
	case fuseops.DirOffset(len(dirents)):
		return nil

	case 0:

	default:
		return fmt.Errorf("Unexpected offset: %v", op.Offset)
	}

	for _, de := range dirents {
		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], de)

		// We don't support doing this in anything more than one shot.
		if n == 0 {
			return fmt.Errorf("Couldn't fit listing in %v bytes", len(op.Dst))
		}

		op.BytesRead += n
	}

	return nil
}

////////////////////////////////////////////////////////////////////////
// File methods
////////////////////////////////////////////////////////////////////////

func (fs *gcsFS) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fmt.Println("OpenFile")

	if op.Inode != gcsFileInodeID {
		return fmt.Errorf("Unsupported inode ID: %d", op.Inode)
	}

	return nil
}

func (fs *gcsFS) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	rc, err := fs.obj.NewReader(ctx)
	defer rc.Close()
	// Currently, only supports small file
	data, err := ioutil.ReadAll(rc)
	if err == io.EOF {
		return nil
	}
	op.BytesRead = copy(op.Dst, data)

	return nil
}

func (fs *gcsFS) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.writer == nil {
		// TODO: fix this hack
		// Ideally, this should be done in OpenFile based on the open mode
		// i.e., read or write
		// However, I didn't see such arg in OpenFileOp
		wctx := context.Background()
		wctx, _ = context.WithTimeout(wctx, time.Second*50)
		fs.writer = fs.obj.NewWriter(wctx)
	}
	// TODO: buffer the data, the network IO speed should be lower
	// don't let the kernel wait for the network IO.
	// As a result, buffering the data can improve write throughput
	fs.writer.Write(op.Data)
	return nil
}

func (fs *gcsFS) SyncFile(
	ctx context.Context,
	op *fuseops.SyncFileOp) error {
	fmt.Println("sync")
	return nil
}

func (fs *gcsFS) FlushFile(
	ctx context.Context,
	op *fuseops.FlushFileOp) error {
	fmt.Println("flush")
	return nil
}

func (fs *gcsFS) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) error {
	fmt.Println("create")
	return nil
}

func (fs *gcsFS) Rename(context.Context, *fuseops.RenameOp) error {
	fmt.Println("Rename")
	return nil
}

func (fs *gcsFS) Unlink(context.Context, *fuseops.UnlinkOp) error {
	fmt.Println("unlink")
	return nil
}

func (fs *gcsFS) MkNode(context.Context, *fuseops.MkNodeOp) error {
	fmt.Println("Mknode")
	return nil
}

func (fs *gcsFS) ReleaseFileHandle(context.Context, *fuseops.ReleaseFileHandleOp) error {
	fmt.Println("ReleaseFileHandle")
	if fs.writer != nil {
		fs.writer.Close()
		fs.writer = nil
	}
	return nil
}

func (fs *gcsFS) SetInodeAttributes(context.Context, *fuseops.SetInodeAttributesOp) error {
	fmt.Println("SetInodeAtt")
	return nil
}