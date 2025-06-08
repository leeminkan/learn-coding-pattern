package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"golang.org/x/exp/constraints"
)

// In a real database, leaf node pointers are TIDs (Tuple Identifiers)
// which point to the row's physical location. We will simulate this
// by storing the byte offset of the row in our data file.
type RecordOffset int64

// Node represents a node in the B+ Tree.
type Node[K constraints.Ordered] struct {
	isLeaf   bool
	keys     []K
	pointers []interface{} // Can point to *Node[K] or RecordOffset
	parent   *Node[K]
	next     *Node[K] // Pointer to the next leaf node
}

// BPlusTree represents the entire B+ Tree structure.
type BPlusTree[K constraints.Ordered] struct {
	root   *Node[K]
	degree int // Also known as 'order'. The max number of pointers from a node.
}

// NewBPlusTree creates and initializes a new B+ Tree.
func NewBPlusTree[K constraints.Ordered](degree int) *BPlusTree[K] {
	if degree < 3 {
		panic("B+ Tree degree must be at least 3")
	}
	return &BPlusTree[K]{
		root:   nil,
		degree: degree,
	}
}

// =================================================================================================
// Search Operations
// =================================================================================================

// Search finds the offset associated with a given key.
func (t *BPlusTree[K]) Search(key K) (RecordOffset, bool) {
	if t.root == nil {
		return 0, false
	}

	leafNode := t.findLeaf(key)
	for i, k := range leafNode.keys {
		if k == key {
			// In a B+ Tree, leaf node pointers are the actual records (or pointers to them).
			return leafNode.pointers[i].(RecordOffset), true
		}
	}
	return 0, false
}

// SearchRange finds all records for keys within the given range [startKey, endKey].
func (t *BPlusTree[K]) SearchRange(startKey, endKey K) []RecordOffset {
	if startKey > endKey {
		return nil
	}

	leafNode := t.findLeaf(startKey)
	var results []RecordOffset

	for leafNode != nil {
		for i, k := range leafNode.keys {
			if k >= startKey && k <= endKey {
				results = append(results, leafNode.pointers[i].(RecordOffset))
			}
			// If we've passed the endKey in a sorted list, we can stop.
			if k > endKey {
				return results
			}
		}
		// Move to the next leaf node using the linked list pointer.
		leafNode = leafNode.next
	}
	return results
}

// findLeaf traverses the tree to find the appropriate leaf node for a given key.
func (t *BPlusTree[K]) findLeaf(key K) *Node[K] {
	currentNode := t.root
	for !currentNode.isLeaf {
		i := 0
		for i < len(currentNode.keys) && key >= currentNode.keys[i] {
			i++
		}
		currentNode = currentNode.pointers[i].(*Node[K])
	}
	return currentNode
}

// =================================================================================================
// Insertion Operations
// =================================================================================================

// Insert adds a new key and its record offset into the tree.
func (t *BPlusTree[K]) Insert(key K, offset RecordOffset) {
	// Case 1: The tree is empty.
	if t.root == nil {
		t.root = &Node[K]{
			isLeaf:   true,
			keys:     []K{key},
			pointers: []interface{}{offset},
			parent:   nil,
		}
		return
	}

	leafNode := t.findLeaf(key)

	// Check for duplicates before inserting.
	for _, k := range leafNode.keys {
		if k == key {
			// In a real DB, this might throw a unique constraint violation error.
			fmt.Printf("Key %v already exists. Ignoring insertion.\n", key)
			return
		}
	}

	// Insert into the leaf node.
	t.insertIntoLeaf(leafNode, key, offset)

	// A node splits when the number of keys equals the degree.
	// (Max keys = degree - 1)
	if len(leafNode.keys) == t.degree {
		t.splitAndPromote(leafNode)
	}
}

// insertIntoLeaf inserts a key-offset pair into a leaf node, maintaining sorted order.
func (t *BPlusTree[K]) insertIntoLeaf(node *Node[K], key K, offset RecordOffset) {
	insertPos := 0
	for insertPos < len(node.keys) && key > node.keys[insertPos] {
		insertPos++
	}

	node.keys = append(node.keys[:insertPos], append([]K{key}, node.keys[insertPos:]...)...)
	node.pointers = append(node.pointers[:insertPos], append([]interface{}{offset}, node.pointers[insertPos:]...)...)
}

// splitAndPromote handles the splitting of a node (leaf or internal) and promotes a key to the parent.
func (t *BPlusTree[K]) splitAndPromote(node *Node[K]) {
	splitPoint := t.degree / 2

	newRightNode := &Node[K]{
		isLeaf: node.isLeaf,
		parent: node.parent,
		// Pre-allocate slices with capacity to avoid multiple re-allocations
		keys:     make([]K, 0, t.degree),
		pointers: make([]interface{}, 0, t.degree+1),
	}

	var keyToPromote K

	if node.isLeaf {
		// --- Leaf Node Split Logic ---
		// Copy second half to the new node
		newRightNode.keys = append(newRightNode.keys, node.keys[splitPoint:]...)
		newRightNode.pointers = append(newRightNode.pointers, node.pointers[splitPoint:]...)

		// The key to promote is the first key of the new right node (it's a copy)
		keyToPromote = newRightNode.keys[0]

		// Truncate original node
		node.keys = node.keys[:splitPoint]
		node.pointers = node.pointers[:splitPoint]

		// Link the leaf nodes' sibling pointers
		newRightNode.next = node.next
		node.next = newRightNode
	} else {
		// --- Internal Node Split Logic ---
		// The middle key is *moved up*, not copied
		keyToPromote = node.keys[splitPoint]

		// Copy keys *after* the promoted key to the new right node
		newRightNode.keys = append(newRightNode.keys, node.keys[splitPoint+1:]...)
		// Copy pointers *after* the promoted key's original position
		newRightNode.pointers = append(newRightNode.pointers, node.pointers[splitPoint+1:]...)

		// Truncate original node to hold keys/pointers *before* the promoted key
		node.keys = node.keys[:splitPoint]
		node.pointers = node.pointers[:splitPoint+1] // One more pointer than keys
	}

	// --- Parent Insertion Logic (for both leaf and internal splits) ---
	if node.parent == nil {
		// If the split node was the root, create a new root
		newRoot := &Node[K]{
			isLeaf:   false,
			keys:     []K{keyToPromote},
			pointers: []interface{}{node, newRightNode},
		}
		node.parent = newRoot
		newRightNode.parent = newRoot
		t.root = newRoot
	} else {
		// Insert into existing parent
		parent := node.parent

		// *** CRITICAL FIX ***: Update parent pointer for children moved to the new right node
		if !newRightNode.isLeaf {
			for _, p := range newRightNode.pointers {
				childNode := p.(*Node[K])
				childNode.parent = newRightNode
			}
		}

		t.insertIntoParent(parent, keyToPromote, newRightNode)

		// Recursively split the parent if it is now full
		if len(parent.keys) == t.degree {
			t.splitAndPromote(parent)
		}
	}
}

// insertIntoParent inserts a key and a new child node pointer into an internal node.
func (t *BPlusTree[K]) insertIntoParent(parent *Node[K], key K, newChild *Node[K]) {
	insertPos := 0
	for insertPos < len(parent.keys) && key > parent.keys[insertPos] {
		insertPos++
	}

	parent.keys = append(parent.keys[:insertPos], append([]K{key}, parent.keys[insertPos:]...)...)
	parent.pointers = append(parent.pointers[:insertPos+1], append([]interface{}{newChild}, parent.pointers[insertPos+1:]...)...)
}

// =================================================================================================
// Persistence Operations
// =================================================================================================

// Serializable structs for JSON marshalling. We can't directly serialize pointers.
type SerializableNode[K constraints.Ordered] struct {
	IsLeaf   bool    `json:"isLeaf"`
	Keys     []K     `json:"keys"`
	Pointers []int64 `json:"pointers"` // Will hold NodeIDs or RecordOffsets
	ParentID int     `json:"parentID"`
	NextID   int     `json:"nextID"`
	NodeID   int     `json:"nodeID"`
}

type SerializableTree[K constraints.Ordered] struct {
	Degree int                   `json:"degree"`
	RootID int                   `json:"rootID"`
	Nodes  []SerializableNode[K] `json:"nodes"`
}

// SaveToFile serializes the B+ Tree index to a JSON file.
func (t *BPlusTree[K]) SaveToFile(path string) error {
	if t.root == nil {
		return fmt.Errorf("cannot save an empty tree")
	}

	nodeMap := make(map[*Node[K]]int)
	serializableNodes := []SerializableNode[K]{}
	nodeIDCounter := 0

	// Level-order traversal to assign IDs to each node
	queue := []*Node[K]{t.root}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		if _, exists := nodeMap[node]; !exists {
			nodeMap[node] = nodeIDCounter
			nodeIDCounter++
			if !node.isLeaf {
				for _, p := range node.pointers {
					queue = append(queue, p.(*Node[K]))
				}
			}
		}
	}

	// Create serializable representation of each node
	for node, id := range nodeMap {
		sNode := SerializableNode[K]{
			IsLeaf:   node.isLeaf,
			Keys:     node.keys,
			NodeID:   id,
			ParentID: -1, // Default to -1 (no parent, e.g., root)
			NextID:   -1, // Default to -1 (no next)
		}
		if node.parent != nil {
			sNode.ParentID = nodeMap[node.parent]
		}
		if node.next != nil {
			sNode.NextID = nodeMap[node.next]
		}

		for _, p := range node.pointers {
			if node.isLeaf {
				sNode.Pointers = append(sNode.Pointers, int64(p.(RecordOffset)))
			} else {
				sNode.Pointers = append(sNode.Pointers, int64(nodeMap[p.(*Node[K])]))
			}
		}
		serializableNodes = append(serializableNodes, sNode)
	}

	serializableTree := SerializableTree[K]{
		Degree: t.degree,
		RootID: nodeMap[t.root],
		Nodes:  serializableNodes,
	}

	file, err := json.MarshalIndent(serializableTree, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, file, 0644)
}

// LoadFromFile deserializes a B+ Tree index from a JSON file.
func LoadFromFile[K constraints.Ordered](path string) (*BPlusTree[K], error) {
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var sTree SerializableTree[K]
	if err := json.Unmarshal(file, &sTree); err != nil {
		return nil, err
	}

	tree := NewBPlusTree[K](sTree.Degree)
	if len(sTree.Nodes) == 0 {
		return tree, nil
	}

	// First pass: create all nodes and store them in a map by their ID
	nodeMapByID := make(map[int]*Node[K])
	for _, sNode := range sTree.Nodes {
		node := &Node[K]{
			isLeaf: sNode.IsLeaf,
			keys:   sNode.Keys,
		}
		nodeMapByID[sNode.NodeID] = node
	}

	// Second pass: link all the nodes together using the map
	for _, sNode := range sTree.Nodes {
		node := nodeMapByID[sNode.NodeID]
		if sNode.ParentID != -1 {
			node.parent = nodeMapByID[sNode.ParentID]
		}
		if sNode.NextID != -1 {
			node.next = nodeMapByID[sNode.NextID]
		}

		for _, pID := range sNode.Pointers {
			if node.isLeaf {
				node.pointers = append(node.pointers, RecordOffset(pID))
			} else {
				node.pointers = append(node.pointers, nodeMapByID[int(pID)])
			}
		}
	}

	tree.root = nodeMapByID[sTree.RootID]
	return tree, nil
}

// =================================================================================================
// Utility and Print Functions
// =================================================================================================

// PrintTree provides a simple visualization of the tree structure.
func (t *BPlusTree[K]) PrintTree() {
	if t.root == nil {
		fmt.Println("Tree is empty.")
		return
	}
	queue := []*Node[K]{t.root}
	level := 0
	for len(queue) > 0 {
		levelSize := len(queue)
		fmt.Printf("Level %d: ", level)
		for i := 0; i < levelSize; i++ {
			node := queue[0]
			queue = queue[1:]

			fmt.Printf("%v ", node.keys)
			if !node.isLeaf {
				for _, p := range node.pointers {
					queue = append(queue, p.(*Node[K]))
				}
			}
		}
		fmt.Println()
		level++
	}
}

// =================================================================================================
// Main Function to Demonstrate Usage
// =================================================================================================

func buildTreeFromFile(tree *BPlusTree[int], dataFilePath string) error {
	file, err := os.Open(dataFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var offset int64 = 0

	// Skip header line
	line, _, err := reader.ReadLine()
	if err != nil {
		return err
	}
	offset += int64(len(line)) + 1 // +1 for the newline character

	// Read data lines
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		parts := strings.Split(string(line), ",")
		if len(parts) < 1 {
			continue
		}
		id, err := strconv.Atoi(parts[0])
		if err != nil {
			continue // Skip lines with invalid IDs
		}

		// Insert the ID as the key and the line's starting offset as the value
		tree.Insert(id, RecordOffset(offset))

		offset += int64(len(line)) + 1
	}
	return nil
}

func readDataAtOffset(dataFilePath string, offset RecordOffset) (string, error) {
	file, err := os.Open(dataFilePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	line, _, err := reader.ReadLine()
	if err != nil {
		return "", err
	}
	return string(line), nil
}

func main() {
	const dataFile = "users.csv"
	const indexFile = "users_pk.idx"
	degree := 4
	tree := NewBPlusTree[int](degree)

	fmt.Println("--- Building B+ Tree index from users.csv ---")
	if err := buildTreeFromFile(tree, dataFile); err != nil {
		panic(err)
	}
	fmt.Println("Index built successfully in memory.")

	fmt.Println("\n--- Let's see the final tree structure ---")
	tree.PrintTree()

	fmt.Println("\n--- Use Case 1: Point Search (Find user with id=12) ---")
	offset, found := tree.Search(12)
	if found {
		fmt.Printf("Found key 12. Stored offset is: %d\n", offset)
		rowData, err := readDataAtOffset(dataFile, offset)
		if err == nil {
			fmt.Printf("Data at offset: %s\n", rowData)
		}
	} else {
		fmt.Println("Key 12 not found.")
	}

	fmt.Println("\n--- Use Case 2: Range Search (Find users with id between 5 and 8) ---")
	offsets := tree.SearchRange(5, 8)
	fmt.Printf("Found %d records in range [5, 8]. Offsets: %v\n", len(offsets), offsets)
	for _, off := range offsets {
		rowData, _ := readDataAtOffset(dataFile, off)
		fmt.Printf("  - Data at offset %d: %s\n", off, rowData)
	}

	fmt.Println("\n--- Use Case 3: Saving the index to a file ---")
	if err := tree.SaveToFile(indexFile); err != nil {
		panic(err)
	}
	fmt.Printf("Index saved to %s\n", indexFile)

	fmt.Println("\n--- Use Case 4: Loading the index from file into a new tree ---")
	loadedTree, err := LoadFromFile[int](indexFile)
	if err != nil {
		panic(err)
	}
	fmt.Println("Index loaded successfully.")

	fmt.Println("\n--- Verifying loaded tree with same searches ---")
	fmt.Println("Point Search for id=12 on loaded tree:")
	offset, found = loadedTree.Search(12)
	if found {
		rowData, _ := readDataAtOffset(dataFile, offset)
		fmt.Printf("Found key 12. Data: %s\n", rowData)
	}

	fmt.Println("Range Search for id between 5 and 8 on loaded tree:")
	offsets = loadedTree.SearchRange(5, 8)
	fmt.Printf("Found %d records.\n", len(offsets))
	for _, off := range offsets {
		rowData, _ := readDataAtOffset(dataFile, off)
		fmt.Printf("  - Data: %s\n", rowData)
	}
}
