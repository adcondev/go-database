// package main implements simple file operations demonstrating
// techniques of updating files in-place and atomic renaming.
package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
)

var (
	WriteErr  error = errors.New("write: bytes not written")
	OpenErr   error = errors.New("open: file not opened")
	SyncErr   error = errors.New("sync: data not persisted")
	FolderErr error = errors.New("folder: path not created")
)

// ===
// Chapter 01: From Files To Databases
// 1.1 Updating files in-place
// ===

// SaveData1 Save some data to disk.
//
// Limitations:
//  1. It updates the content as a whole; only usable for tiny data.
//  2. If you need to update the old file, you must read and modify it in memory, then overwrite the old file.
//  3. You need a server to coordinate concurrent clients.
func SaveData1(path, file string, data []byte) error {
	// Let’s say you need to save some data to disk; this is a typical way to do it
	err := os.MkdirAll(path, 0755) // Ensure the directory exists
	if err != nil {
		return FolderErr
	}
	fp, err := os.OpenFile(
		// write-only, create a new file if none exists, truncate regular writable file when opened
		path+file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664,
	)
	if err != nil {
		return OpenErr
	}
	defer fp.Close()

	_, err = fp.Write(data) // Writes data
	if err != nil {
		return WriteErr
	}
	return fp.Sync() // data is not persistent until fp.Sync() call
}

// ===
// Chapter 01: From Files To Databases
// 1.2 Atomic Renaming
// ===

// SaveData2 Replacing data atomically by renaming files
//
// Not touching the old file data means:
//  1. If the update is interrupted, you can recover from the old file since it remains intact.
//  2. Concurrent readers won’t get half written data.
//
// Atomicity:
//   - Rename is atomic w.r.t. concurrent readers; a reader opens either the old or the new file.
//   - Rename is NOT atomic w.r.t. power loss; it’s not even durable.
func SaveData2(path, file string, data []byte) error {
	// Many problems are solved by not updating data in-place.
	// You can write a new file and delete the old file.
	stamp := strconv.Itoa(rand.Int())
	tmp := fmt.Sprintf("%s.tmp.%s", path+file, stamp)

	err := os.MkdirAll(path, 0755) // Ensure the directory exists
	if err != nil {
		return FolderErr
	}

	fp, err := os.OpenFile(
		// write-only, create a new file if none exists, file must not exist
		tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664,
	)
	if err != nil {
		return OpenErr
	}
	defer func() {
		fp.Close()
		if err != nil {
			os.Remove(tmp) // If any error, deletes new file.
		}
	}()

	_, err = fp.Write(data) // Write
	if err != nil {
		return WriteErr
	}
	err = fp.Sync() // Persist data
	if err != nil {
		return SyncErr
	}
	// Renaming a file to an existing one replaces it atomically;
	// deleting the old file is not needed (and not correct).
	return os.Rename(tmp, path)
}

func main() {
	path := "./cmd/files/"
	file := "hello_world.txt"
	data := []byte("Hello, World!")
	err := SaveData1(path, file, data)
	if err != nil {
		log.Printf("NOT CREATED! %v", err)
	}
	// Update the file atomically
	data = []byte("Bye, World!")
	err = SaveData2(path, file, data)
	if err != nil {
		log.Printf("NOT EDITED! %v", err)
	}
}
