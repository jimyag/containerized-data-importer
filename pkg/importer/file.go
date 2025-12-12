package importer

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"

	"k8s.io/klog/v2"
)

// isZeroBlock checks if a byte slice contains only zeros.
// Uses 64-bit word comparison for better performance on large buffers.
func isZeroBlock(buf []byte) bool {
	if len(buf) == 0 {
		return true
	}

	// Fast path: check using 64-bit words
	// This is ~8x faster than byte-by-byte comparison
	ptr := unsafe.Pointer(&buf[0])
	n := len(buf)

	// Check 64-bit aligned portion
	words := n / 8
	wordPtr := (*[1 << 30]uint64)(ptr)[:words:words]
	for _, w := range wordPtr {
		if w != 0 {
			return false
		}
	}

	// Check remaining bytes
	for i := words * 8; i < n; i++ {
		if buf[i] != 0 {
			return false
		}
	}
	return true
}

var (
	blockdevFileName = "/usr/sbin/blockdev"
)

// OpenFileOrBlockDevice opens the destination data file, whether it is a block device or regular file
func OpenFileOrBlockDevice(fileName string) (*os.File, error) {
	var outFile *os.File
	blockSize, err := GetAvailableSpaceBlock(fileName)
	if err != nil {
		return nil, errors.Wrapf(err, "error determining if block device exists")
	}
	if blockSize >= 0 {
		// Block device found and size determined.
		outFile, err = os.OpenFile(fileName, os.O_EXCL|os.O_WRONLY, os.ModePerm)
	} else {
		// Attempt to create the file with name filePath.  If it exists, fail.
		outFile, err = os.OpenFile(fileName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.ModePerm)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "could not open file %q", fileName)
	}
	return outFile, nil
}

// LinkFile symlinks the source to the target
func LinkFile(source, target string) error {
	out, err := exec.Command("/usr/bin/ln", "-s", source, target).CombinedOutput()
	if err != nil {
		fmt.Printf("out [%s]\n", string(out))
		return err
	}
	return nil
}

// GetAvailableSpace gets the amount of available space at the path specified.
func GetAvailableSpace(path string) (int64, error) {
	var stat syscall.Statfs_t
	err := syscall.Statfs(path, &stat)
	if err != nil {
		return int64(-1), err
	}
	//nolint:unconvert
	return int64(stat.Bavail) * int64(stat.Bsize), nil
}

// GetAvailableSpaceBlock gets the amount of available space at the block device path specified.
func GetAvailableSpaceBlock(deviceName string) (int64, error) {
	// Check if the file exists and is a device file.
	if ok, err := IsDevice(deviceName); !ok || err != nil {
		return int64(-1), err
	}

	// Device exists, attempt to get size.
	cmd := exec.Command(blockdevFileName, "--getsize64", deviceName)
	var out bytes.Buffer
	var errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	err := cmd.Run()
	if err != nil {
		return int64(-1), errors.Errorf("%v, %s", err, errBuf.String())
	}
	i, err := strconv.ParseInt(strings.TrimSpace(out.String()), 10, 64)
	if err != nil {
		return int64(-1), err
	}
	return i, nil
}

// IsDevice returns true if it's a device file
func IsDevice(deviceName string) (bool, error) {
	info, err := os.Stat(deviceName)
	if err == nil {
		return (info.Mode() & os.ModeDevice) != 0, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

// Three functions for zeroing a range in the destination file:

// PunchHole attempts to zero a range in a file with fallocate, for block devices and pre-allocated files.
func PunchHole(outFile *os.File, start, length int64) error {
	klog.V(4).Infof("Punching %d-byte hole at offset %d", length, start)
	flags := uint32(unix.FALLOC_FL_PUNCH_HOLE | unix.FALLOC_FL_KEEP_SIZE)
	err := syscall.Fallocate(int(outFile.Fd()), flags, start, length)
	if err == nil {
		_, err = outFile.Seek(length, io.SeekCurrent) // Just to move current file position
	}
	return err
}

// unit test support to simulate failure and fallback to zero writes
var appendZeroWithTruncateFunc = AppendZeroWithTruncate

// AppendZeroWithTruncate resizes the file to append zeroes, meant only for newly-created (empty and zero-length) regular files.
func AppendZeroWithTruncate(outFile *os.File, start, length int64) error {
	klog.V(4).Infof("Truncating %d-bytes from offset %d", length, start)
	end, err := outFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if start != end {
		return errors.Errorf("starting offset %d does not match previous ending offset %d, cannot safely append zeroes to this file using truncate", start, end)
	}
	err = outFile.Truncate(start + length)
	if err != nil {
		return err
	}
	_, err = outFile.Seek(0, io.SeekEnd)
	return err
}

var zeroBuffer []byte

// AppendZeroWithWrite just does normal file writes to the destination, a slow but reliable fallback option.
func AppendZeroWithWrite(outFile *os.File, start, length int64) error {
	klog.Infof("Writing %d zero bytes at offset %d", length, start)
	offset, err := outFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if start != offset {
		return errors.Errorf("starting offset %d does not match previous ending offset %d, cannot safely append zeroes to this file using write", start, offset)
	}
	if zeroBuffer == nil { // No need to re-allocate this on every write
		zeroBuffer = bytes.Repeat([]byte{0}, 32<<20)
	}
	count := int64(0)
	for count < length {
		blockSize := int64(len(zeroBuffer))
		remaining := length - count
		if remaining < blockSize {
			blockSize = remaining
		}
		written, err := outFile.Write(zeroBuffer[:blockSize])
		if err != nil {
			return errors.Wrapf(err, "unable to write %d zeroes at offset %d: %v", length, start+count, err)
		}
		count += int64(written)
	}
	return nil
}

func StreamDataToFile(r io.Reader, fileName string, preallocate bool) (int64, int64, error) {
	var outFile *os.File
	var bytesRead, bytesWritten int64
	outFile, err := OpenFileOrBlockDevice(fileName)
	if err != nil {
		return 0, 0, err
	}
	defer outFile.Close()

	if !preallocate {
		var isDevice bool
		zeroWriter := appendZeroWithTruncateFunc
		isDevice, err = IsDevice(fileName)
		if err != nil {
			return 0, 0, err
		}

		if isDevice {
			zeroWriter = PunchHole
		}

		bytesRead, bytesWritten, err = copyWithSparseCheck(outFile, r, zeroWriter)
	} else {
		bytesRead, err = io.Copy(outFile, r)
		bytesWritten = bytesRead
	}

	defer func() {
		klog.Infof("Read %d bytes, wrote %d bytes to %s", bytesRead, bytesWritten, outFile.Name())
	}()

	if err != nil {
		klog.Errorf("Unable to write file from dataReader: %v\n", err)
		os.Remove(outFile.Name())
		if IsNoCapacityError(err) {
			return bytesRead, bytesWritten, fmt.Errorf("unable to write to file: %w", err)
		}
		return bytesRead, bytesWritten, NewImagePullFailedError(err)
	}

	err = outFile.Sync()

	return bytesRead, bytesWritten, err
}

type zeroWriterFunc func(*os.File, int64, int64) error

func zeroWriterWithFallback(zwf zeroWriterFunc) func(dst *os.File, start, length int64) (int64, error) {
	return func(dst *os.File, start, length int64) (int64, error) {
		err := zwf(dst, start, length)
		if err != nil {
			klog.Errorf("Error zeroing range in destination file: %v, will write zeros directly", err)
			err = AppendZeroWithWrite(dst, start, length)
			if err != nil {
				return 0, err
			}
			return length, nil
		}
		return 0, nil
	}
}

func copyWithSparseCheck(dst *os.File, src io.Reader, zeroWriter zeroWriterFunc) (int64, int64, error) {
	klog.Infof("copyWithSparseCheck to %s", dst.Name())
	const (
		buffSize       = 4 << 20  // 4 MB buffer size (sparse detection boundary)
		syncInterval   = 64 << 20 // sync every 64 MB read (not written, to handle sparse files)
		skipWrite      = false    // DEBUG: skip actual write to test download+decompress speed
		skipZeroCheck  = false    // DEBUG: skip zero detection to test pure download+decompress speed
	)
	var bytesRead, bytesWritten int64
	var bytesReadSinceLastSync int64
	var syncCount int
	var lastLogTime time.Time
	writeBuf := make([]byte, buffSize)
	var writeOffset int64
	checkZeros := true
	zeroWriterFunc := zeroWriterWithFallback(zeroWriter)
	for {
		nr, er := src.Read(writeBuf)
		if nr > 0 {
			var nw int
			var ew error
			var zbw int64
			// Use optimized zero detection (64-bit word comparison)
			isZero := !skipZeroCheck && checkZeros && isZeroBlock(writeBuf[0:nr])
			if isZero {
				bytesRead += int64(nr)
				bytesReadSinceLastSync += int64(nr)
			} else {
				if skipWrite {
					// DEBUG: skip write, just count bytes
					bytesRead += int64(nr)
					bytesReadSinceLastSync += int64(nr)
					bytesWritten += int64(nr)
					writeOffset = bytesRead
				} else {
					if bytesRead > writeOffset {
						// func should seek to bytesRead before returning
						zbw, ew = zeroWriterFunc(dst, writeOffset, bytesRead-writeOffset)
						if ew != nil {
							klog.Errorf("Error writing zeroes to destination file: %v", ew)
							return bytesRead, bytesWritten, ew
						}
						bytesWritten += zbw
						if zbw > 0 {
							checkZeros = false
						}
					}
					nw, ew = dst.Write(writeBuf[0:nr])
					if nw < 0 || nr < nw {
						nw = 0
						if ew == nil {
							ew = fmt.Errorf("invalid write result")
						}
					}
					bytesRead += int64(nr)
					bytesReadSinceLastSync += int64(nr)
					bytesWritten += int64(nw)
					writeOffset = bytesRead
					if ew != nil {
						return bytesRead, bytesWritten, ew
					}
					if nr != nw {
						return bytesRead, bytesWritten, io.ErrShortWrite
					}
				}
			}
			// Periodic log based on bytes read
			if bytesReadSinceLastSync >= syncInterval {
				if !skipWrite {
					syncStart := time.Now()
					if err := dst.Sync(); err != nil {
						return bytesRead, bytesWritten, errors.Wrap(err, "periodic sync failed")
					}
					syncCount++
					// Log sync stats every second
					if time.Since(lastLogTime) >= time.Second {
						klog.Infof("Sync #%d took %v, bytesRead: %d MB, bytesWritten: %d MB",
							syncCount, time.Since(syncStart), bytesRead>>20, bytesWritten>>20)
						lastLogTime = time.Now()
					}
				} else {
					// DEBUG: just log progress
					if time.Since(lastLogTime) >= time.Second {
						klog.Infof("[SKIP-WRITE] bytesRead: %d MB, bytesWritten: %d MB",
							bytesRead>>20, bytesWritten>>20)
						lastLogTime = time.Now()
					}
				}
				bytesReadSinceLastSync = 0
			}
		}
		if er != nil {
			if er != io.EOF {
				return bytesRead, bytesWritten, er
			}
			break
		}
	}
	if bytesRead > writeOffset && !skipWrite {
		zbw, err := zeroWriterFunc(dst, writeOffset, bytesRead-writeOffset)
		if err != nil {
			klog.Errorf("Error writing zeroes to destination file: %v", err)
			return bytesRead, bytesWritten, err
		}
		bytesWritten += zbw
	}
	klog.Infof("[DONE] skipWrite=%v, bytesRead: %d MB, bytesWritten: %d MB", skipWrite, bytesRead>>20, bytesWritten>>20)
	return bytesRead, bytesWritten, nil
}
