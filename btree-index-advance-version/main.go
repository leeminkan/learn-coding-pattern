package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// =================================================================================================
// --- main.go --- (Demonstration)
// =================================================================================================

// visualizeIndexFile reads the binary index file and prints its structure.
func visualizeIndexFile(indexFilePath string) error {
	fmt.Println("\n--- Visualizing On-Disk Index File Structure ---")
	pager, err := NewPager(indexFilePath)
	if err != nil {
		return err
	}
	defer pager.Close()
	if pager.numPages == 0 {
		fmt.Println("Index file is empty.")
		return nil
	}

	for i := int64(0); i < pager.numPages; i++ {
		pageID := PageID(i)
		page, err := pager.ReadPage(pageID, new(Page))
		if err != nil {
			fmt.Printf("Error reading page %d: %v\n", pageID, err)
			continue
		}

		nodeType := "INTERNAL"
		if isLeaf(page) {
			nodeType = "LEAF"
		}
		numKeys := getNumKeys(page)
		parentID := getParentPageID(page)
		fmt.Printf("\n[ Page %d | Type: %s | NumKeys: %d | ParentID: %d ]\n", pageID, nodeType, numKeys, parentID)

		if isLeaf(page) {
			nextID := getNextLeafPageID(page)
			fmt.Printf("  - Header: NextLeafID -> %d\n", nextID)
			fmt.Println("  - Content: [Key -> RecordOffset]")
			for j := 0; j < int(numKeys); j++ {
				offset := headerSize + j*(8+8)
				key := binary.LittleEndian.Uint64(page[offset:])
				value := binary.LittleEndian.Uint64(page[offset+8:])
				fmt.Printf("    - %d -> %d\n", key, value)
			}
		} else {
			fmt.Println("  - Content: [PtrToPageID | Key | PtrToPageID | ...]")
			ptrOffset := headerSize
			ptr := binary.LittleEndian.Uint64(page[ptrOffset:])
			fmt.Printf("    - Ptr -> %d\n", ptr)
			for j := 0; j < int(numKeys); j++ {
				keyOffset := headerSize + j*(8+8) + 8
				ptrOffset := keyOffset + 8
				key := binary.LittleEndian.Uint64(page[keyOffset:])
				ptr := binary.LittleEndian.Uint64(page[ptrOffset:])
				fmt.Printf("    - Key: %d\n", key)
				fmt.Printf("    - Ptr -> %d\n", ptr)
			}
		}
	}
	return nil
}

func readDataAtOffset(dataFilePath string, offset int64) (string, error) {
	file, err := os.Open(dataFilePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return "", err
	}
	line, _, err := bufio.NewReader(file).ReadLine()
	if err != nil {
		return "", err
	}
	return string(line), nil
}

// buildTreeFromFile now uses the dynamic Insert function.
func buildTreeFromFile(tree *BPlusTree, dataFilePath string) error {
	dataFile, err := os.Open(dataFilePath)
	if err != nil {
		return err
	}
	defer dataFile.Close()
	reader := bufio.NewReader(dataFile)
	var offset int64 = 0
	line, _, err := reader.ReadLine()
	if err != nil {
		return err
	}
	offset += int64(len(line)) + 1

	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		parts := strings.Split(string(line), ",")
		if len(parts) > 0 {
			id, convErr := strconv.Atoi(parts[0])
			if convErr == nil {
				if insertErr := tree.Insert(id, offset); insertErr != nil {
					return insertErr
				}
			}
		}
		offset += int64(len(line)) + 1
	}
	return nil
}

func main() {
	const dataFile = "users.csv"
	const indexFile = "users_pk.idx"
	// Let's use a small degree to force splits quickly for demonstration
	// Max keys per node will be degree - 1.
	const degree = 4

	// Clean up old index file if it exists
	os.Remove(indexFile)

	// --- Step 1: Create a new BTree handle linked to our index file ---
	pager, err := NewPager(indexFile)
	if err != nil {
		panic(err)
	}
	// We defer the close on the main function's pager to ensure the file is closed at the end.
	defer pager.Close()
	tree := NewBPlusTree(pager, degree)

	// --- Step 2: Build the B+ Tree index dynamically by inserting from users.csv ---
	fmt.Println("--- Building B+ Tree index dynamically from users.csv ---")
	if err := buildTreeFromFile(tree, dataFile); err != nil {
		panic(err)
	}
	fmt.Println("Index build process finished.")

	// --- Step 3: Visualize the final binary index file structure ---
	visualizeIndexFile(indexFile)

	// --- Step 4: Use the dynamically built index for queries ---
	fmt.Println("\n--- Use Case 1: Point Search (Find user with id=12) ---")
	keyToFind := 12
	offset, found, err := tree.Search(keyToFind)
	if err != nil {
		panic(err)
	}
	if found {
		fmt.Printf("Index Search found key %d. Stored offset is: %d\n", keyToFind, offset)
		rowData, _ := readDataAtOffset(dataFile, offset)
		fmt.Printf("Data at offset from '%s': %s\n", dataFile, rowData)
	} else {
		fmt.Printf("Key %d not found.\n", keyToFind)
	}

	// --- Step 5: Use the Range Search implementation ---
	fmt.Println("\n--- Use Case 2: Range Search (Find users with id between 5 and 8) ---")
	offsets, err := tree.SearchRange(5, 8)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Found %d records in range [5, 8].\n", len(offsets))
	for _, off := range offsets {
		rowData, _ := readDataAtOffset(dataFile, off)
		fmt.Printf("  - Data at offset %d: %s\n", off, rowData)
	}
}
