package main

import (
	"encoding/binary"
	"fmt"
)

// =================================================================================================
// --- btree.go --- (B+ Tree Logic using Pages)
// =================================================================================================

// On-disk page header structure constants
const (
	nodeTypeOffset    = 0
	isRootOffset      = 1
	parentPtrOffset   = 8
	numKeysOffset     = 16
	nextLeafPtrOffset = 24
	headerSize        = 32
)

const (
	NodeTypeLeaf     = 0
	NodeTypeInternal = 1
)

// BPlusTree struct and NewBPlusTree constructor
type BPlusTree struct {
	pager      *Pager
	rootPageID PageID
	degree     int
}

func NewBPlusTree(pager *Pager, degree int) *BPlusTree {
	if degree < 3 {
		panic("B+ Tree degree must be at least 3")
	}
	if pager.numPages == 0 {
		rootPageData := new(Page)
		rootPageData[nodeTypeOffset] = NodeTypeLeaf
		setIsRoot(rootPageData, true)
		setParentPageID(rootPageData, -1) // Root's parent is invalid
		setNumKeys(rootPageData, 0)
		setNextLeafPageID(rootPageData, -1)
		pager.WritePage(0, rootPageData)
		return &BPlusTree{pager: pager, rootPageID: 0, degree: degree}
	}
	// In a real DB, we'd read a master page to find the rootPageID.
	// We'll assume it's always 0 for this simulation if the file exists.
	// You would need to query the Pager to find the root page ID.
	return &BPlusTree{pager: pager, rootPageID: 0, degree: degree}
}

// Helper functions for page metadata
func isLeaf(page *Page) bool       { return page[nodeTypeOffset] == NodeTypeLeaf }
func getNumKeys(page *Page) uint16 { return binary.LittleEndian.Uint16(page[numKeysOffset:]) }
func setNumKeys(page *Page, numKeys uint16) {
	binary.LittleEndian.PutUint16(page[numKeysOffset:], numKeys)
}
func getNextLeafPageID(page *Page) PageID {
	return PageID(binary.LittleEndian.Uint64(page[nextLeafPtrOffset:]))
}
func setNextLeafPageID(page *Page, pageID PageID) {
	binary.LittleEndian.PutUint64(page[nextLeafPtrOffset:], uint64(pageID))
}
func isRoot(page *Page) bool { return page[isRootOffset] == 1 }
func setIsRoot(page *Page, isRoot bool) {
	if isRoot {
		page[isRootOffset] = 1
	} else {
		page[isRootOffset] = 0
	}
}
func getParentPageID(page *Page) PageID {
	return PageID(binary.LittleEndian.Uint64(page[parentPtrOffset:]))
}
func setParentPageID(page *Page, pageID PageID) {
	binary.LittleEndian.PutUint64(page[parentPtrOffset:], uint64(pageID))
}

// Search and SearchRange (no changes needed)
func (t *BPlusTree) Search(key int) (int64, bool, error) {
	leafPageID, err := t.findLeafPage(key)
	if err != nil {
		return 0, false, err
	}
	page, err := t.pager.ReadPage(leafPageID, new(Page))
	if err != nil {
		return 0, false, err
	}

	numKeys := int(getNumKeys(page))
	for i := 0; i < numKeys; i++ {
		offset := headerSize + i*(8+8)
		if int(binary.LittleEndian.Uint64(page[offset:])) == key {
			return int64(binary.LittleEndian.Uint64(page[offset+8:])), true, nil
		}
	}
	return 0, false, nil
}

func (t *BPlusTree) SearchRange(startKey, endKey int) ([]int64, error) {
	if startKey > endKey {
		return nil, nil
	}
	leafPageID, err := t.findLeafPage(startKey)
	if err != nil {
		return nil, err
	}
	var results []int64
	for leafPageID != -1 {
		page, err := t.pager.ReadPage(leafPageID, new(Page))
		if err != nil {
			return nil, err
		}
		numKeys := int(getNumKeys(page))
		foundEnd := false
		for i := 0; i < numKeys; i++ {
			offset := headerSize + i*(8+8)
			currentKey := int(binary.LittleEndian.Uint64(page[offset:]))
			if currentKey > endKey {
				foundEnd = true
				break
			}
			if currentKey >= startKey {
				results = append(results, int64(binary.LittleEndian.Uint64(page[offset+8:])))
			}
		}
		if foundEnd {
			break
		}
		leafPageID = getNextLeafPageID(page)
	}
	return results, nil
}

// findLeafPage (no changes needed)
func (t *BPlusTree) findLeafPage(key int) (PageID, error) {
	currentPageID := t.rootPageID
	for {
		page, err := t.pager.ReadPage(currentPageID, new(Page))
		if err != nil {
			return -1, err
		}
		if isLeaf(page) {
			return currentPageID, nil
		}
		numKeys := int(getNumKeys(page))
		i := 0
		for i < numKeys {
			offset := headerSize + i*(8+8) + 8
			if key < int(binary.LittleEndian.Uint64(page[offset:])) {
				break
			}
			i++
		}
		offset := headerSize + i*(8+8)
		currentPageID = PageID(binary.LittleEndian.Uint64(page[offset:]))
	}
}

// ==================================
// --- FULL INSERT IMPLEMENTATION ---
// ==================================

// Insert orchestrates the insertion process.
func (t *BPlusTree) Insert(key int, value int64) error {
	leafPageID, err := t.findLeafPage(key)
	if err != nil {
		return err
	}
	leafPage, err := t.pager.ReadPage(leafPageID, new(Page))
	if err != nil {
		return err
	}

	numKeys := int(getNumKeys(leafPage))
	// Check for duplicates
	for i := 0; i < numKeys; i++ {
		offset := headerSize + i*16
		if int(binary.LittleEndian.Uint64(leafPage[offset:])) == key {
			return fmt.Errorf("duplicate key insertion not allowed for key %d", key)
		}
	}

	// A leaf node is full if it has degree-1 keys.
	if numKeys < t.degree-1 {
		insertIntoLeaf(leafPage, key, value)
		return t.pager.WritePage(leafPageID, leafPage)
	}

	// Otherwise, split the leaf.
	return t.splitAndInsertLeaf(leafPageID, leafPage, key, value)
}

// insertIntoLeaf writes a key/value pair into a leaf page's byte buffer.
func insertIntoLeaf(page *Page, key int, value int64) {
	numKeys := int(getNumKeys(page))
	insertIndex := 0
	for insertIndex < numKeys && key > int(binary.LittleEndian.Uint64(page[headerSize+insertIndex*16:])) {
		insertIndex++
	}

	// Shift existing keys/values to the right to make space for the new one
	copy(page[headerSize+(insertIndex+1)*16:], page[headerSize+insertIndex*16:(headerSize+(numKeys)*16)])

	offset := headerSize + insertIndex*16
	binary.LittleEndian.PutUint64(page[offset:], uint64(key))
	binary.LittleEndian.PutUint64(page[offset+8:], uint64(value))
	setNumKeys(page, uint16(numKeys+1))
}

// splitAndInsertLeaf handles splitting a full leaf node.
func (t *BPlusTree) splitAndInsertLeaf(oldPageID PageID, oldPage *Page, key int, value int64) error {
	newPageID := t.pager.AllocatePage()
	newPage := new(Page)
	newPage[nodeTypeOffset] = NodeTypeLeaf
	setParentPageID(newPage, getParentPageID(oldPage))

	tempKeys := make([]int, 0, t.degree)
	tempValues := make([]int64, 0, t.degree)

	numKeys := int(getNumKeys(oldPage))
	inserted := false
	for i := 0; i < numKeys; i++ {
		offset := headerSize + i*16
		currentKey := int(binary.LittleEndian.Uint64(oldPage[offset:]))
		if !inserted && key < currentKey {
			tempKeys = append(tempKeys, key)
			tempValues = append(tempValues, value)
			inserted = true
		}
		tempKeys = append(tempKeys, currentKey)
		tempValues = append(tempValues, int64(binary.LittleEndian.Uint64(oldPage[offset+8:])))
	}
	if !inserted {
		tempKeys = append(tempKeys, key)
		tempValues = append(tempValues, value)
	}

	splitPoint := (t.degree) / 2
	leftKeys := tempKeys[:splitPoint]
	leftValues := tempValues[:splitPoint]
	rightKeys := tempKeys[splitPoint:]
	rightValues := tempValues[splitPoint:]

	keyToPromote := rightKeys[0]

	clear(oldPage[headerSize:])
	setNumKeys(oldPage, uint16(len(leftKeys)))
	for i, k := range leftKeys {
		offset := headerSize + i*16
		binary.LittleEndian.PutUint64(oldPage[offset:], uint64(k))
		binary.LittleEndian.PutUint64(oldPage[offset+8:], uint64(leftValues[i]))
	}

	setNumKeys(newPage, uint16(len(rightKeys)))
	for i, k := range rightKeys {
		offset := headerSize + i*16
		binary.LittleEndian.PutUint64(newPage[offset:], uint64(k))
		binary.LittleEndian.PutUint64(newPage[offset+8:], uint64(rightValues[i]))
	}

	setNextLeafPageID(newPage, getNextLeafPageID(oldPage))
	setNextLeafPageID(oldPage, newPageID)

	if err := t.pager.WritePage(oldPageID, oldPage); err != nil {
		return err
	}
	if err := t.pager.WritePage(newPageID, newPage); err != nil {
		return err
	}

	return t.insertIntoParent(getParentPageID(oldPage), oldPageID, keyToPromote, newPageID)
}

// insertIntoParent handles inserting a promoted key into an internal node, splitting if necessary.
func (t *BPlusTree) insertIntoParent(parentPageID, leftChildID PageID, key int, rightChildID PageID) error {
	if parentPageID == -1 {
		newRootPageID := t.pager.AllocatePage()
		newRootPage := new(Page)
		newRootPage[nodeTypeOffset] = NodeTypeInternal
		setIsRoot(newRootPage, true)
		setParentPageID(newRootPage, -1)
		setNumKeys(newRootPage, 1)

		binary.LittleEndian.PutUint64(newRootPage[headerSize:], uint64(leftChildID))
		binary.LittleEndian.PutUint64(newRootPage[headerSize+8:], uint64(key))
		binary.LittleEndian.PutUint64(newRootPage[headerSize+16:], uint64(rightChildID))

		leftChildPage, _ := t.pager.ReadPage(leftChildID, new(Page))
		setIsRoot(leftChildPage, false)
		setParentPageID(leftChildPage, newRootPageID)
		t.pager.WritePage(leftChildID, leftChildPage)

		rightChildPage, _ := t.pager.ReadPage(rightChildID, new(Page))
		setIsRoot(rightChildPage, false)
		setParentPageID(rightChildPage, newRootPageID)
		t.pager.WritePage(rightChildID, rightChildPage)

		if err := t.pager.WritePage(newRootPageID, newRootPage); err != nil {
			return err
		}
		t.rootPageID = newRootPageID
		return nil
	}

	parentPage, err := t.pager.ReadPage(parentPageID, new(Page))
	if err != nil {
		return err
	}
	numKeys := int(getNumKeys(parentPage))

	if numKeys < t.degree-1 {
		insertIndex := 0
		for insertIndex < numKeys {
			offset := headerSize + insertIndex*16 + 8
			if key < int(binary.LittleEndian.Uint64(parentPage[offset:])) {
				break
			}
			insertIndex++
		}

		keyStart := headerSize + insertIndex*16 + 8
		ptrStart := keyStart - 8
		copy(parentPage[ptrStart+32:], parentPage[ptrStart+16:])

		binary.LittleEndian.PutUint64(parentPage[keyStart:], uint64(key))
		binary.LittleEndian.PutUint64(parentPage[keyStart+8:], uint64(rightChildID))

		setNumKeys(parentPage, uint16(numKeys+1))
		return t.pager.WritePage(parentPageID, parentPage)
	}

	// *** FULL INTERNAL NODE SPLIT IMPLEMENTATION ***
	// If parent is full, we must split it too.
	newPageID := t.pager.AllocatePage()
	newPage := new(Page)
	newPage[nodeTypeOffset] = NodeTypeInternal
	setParentPageID(newPage, getParentPageID(parentPage))

	tempKeys := make([]int, 0, t.degree)
	tempPointers := make([]PageID, 0, t.degree+1)

	// Copy existing keys and pointers to temporary slices
	tempPointers = append(tempPointers, PageID(binary.LittleEndian.Uint64(parentPage[headerSize:])))
	for i := 0; i < numKeys; i++ {
		keyOffset := headerSize + i*16 + 8
		ptrOffset := keyOffset + 8
		tempKeys = append(tempKeys, int(binary.LittleEndian.Uint64(parentPage[keyOffset:])))
		tempPointers = append(tempPointers, PageID(binary.LittleEndian.Uint64(parentPage[ptrOffset:])))
	}

	// Insert the new key and child pointer
	insertIndex := 0
	for insertIndex < len(tempKeys) && key > tempKeys[insertIndex] {
		insertIndex++
	}
	tempKeys = append(tempKeys[:insertIndex], append([]int{key}, tempKeys[insertIndex:]...)...)
	tempPointers = append(tempPointers[:insertIndex+1], append([]PageID{rightChildID}, tempPointers[insertIndex+1:]...)...)

	// Split the temporary slices
	splitPoint := (t.degree) / 2
	keyToPromoteAgain := tempKeys[splitPoint]

	leftKeys := tempKeys[:splitPoint]
	rightKeys := tempKeys[splitPoint+1:]
	leftPointers := tempPointers[:splitPoint+1]
	rightPointers := tempPointers[splitPoint+1:]

	// Update the old (left) parent page
	clear(parentPage[headerSize:])
	setNumKeys(parentPage, uint16(len(leftKeys)))
	binary.LittleEndian.PutUint64(parentPage[headerSize:], uint64(leftPointers[0]))
	for i, k := range leftKeys {
		offset := headerSize + i*16 + 8
		binary.LittleEndian.PutUint64(parentPage[offset:], uint64(k))
		binary.LittleEndian.PutUint64(parentPage[offset+8:], uint64(leftPointers[i+1]))
	}

	// Write the new (right) parent page
	setNumKeys(newPage, uint16(len(rightKeys)))
	binary.LittleEndian.PutUint64(newPage[headerSize:], uint64(rightPointers[0]))
	for i, k := range rightKeys {
		offset := headerSize + i*16 + 8
		binary.LittleEndian.PutUint64(newPage[offset:], uint64(k))
		binary.LittleEndian.PutUint64(newPage[offset+8:], uint64(rightPointers[i+1]))
	}

	// Update parent pointers of the children that were moved
	for _, childPageID := range rightPointers {
		childPage, _ := t.pager.ReadPage(childPageID, new(Page))
		setParentPageID(childPage, newPageID)
		t.pager.WritePage(childPageID, childPage)
	}

	if err := t.pager.WritePage(parentPageID, parentPage); err != nil {
		return err
	}
	if err := t.pager.WritePage(newPageID, newPage); err != nil {
		return err
	}

	// Recursively call insertIntoParent for the grandparent
	return t.insertIntoParent(getParentPageID(parentPage), parentPageID, keyToPromoteAgain, newPageID)
}
