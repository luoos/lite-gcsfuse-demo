package gcsfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
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
	reader *storage.Reader
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
	fmt.Println("ReadFile, offset: ", op.Offset)
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.reader == nil {
		r, err := fs.obj.NewReader(context.Background())
		fs.reader = r
		if err != nil {
			panic(err)
		}
	}
	br, err := fs.reader.Read(op.Dst)
	op.BytesRead = br
	// Currently, only supports small file
	if err == io.EOF {
		return nil
	} else if err != nil {
		panic(err)
	}

	return nil
}

func (fs *gcsFS) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) error {
	fmt.Println("WriteFile, offset: ", op.Offset)
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.writer == nil {
		wctx, _ := context.WithTimeout(context.Background(), time.Second*500)
		fs.writer = fs.obj.NewWriter(wctx)
	}
	// TODO: the write offset is out of order.
	// So the file written to GCS is not right, the MD5 check sum is not right
	byteWritten, err := fs.writer.Write(op.Data)
	if err != nil {
		fmt.Printf("Write err: %v\n", err)
	}

	if l := len(op.Data); l != byteWritten {
		fmt.Printf("Write length mismatch, wrote length: %v, want: %v \n", byteWritten, l)
	}
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
	if fs.reader != nil {
		fs.reader.Close()
		fs.reader = nil
	}
	return nil
}

func (fs *gcsFS) SetInodeAttributes(context.Context, *fuseops.SetInodeAttributesOp) error {
	fmt.Println("SetInodeAtt")
	return nil
}