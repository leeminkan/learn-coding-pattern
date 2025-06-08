package main

import (
	"fmt"
	"os"
)

// =================================================================================================
// --- pager.go --- (DiskManager Simulation)
// =================================================================================================

const PageSize = 4096 // A typical page size

type PageID int64
type Page [PageSize]byte

type Pager struct {
	file     *os.File
	fileSize int64
	numPages int64
}

func NewPager(path string) (*Pager, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := stat.Size()
	numPages := fileSize / PageSize

	return &Pager{
		file:     file,
		fileSize: fileSize,
		numPages: numPages,
	}, nil
}

func (p *Pager) ReadPage(pageID PageID, pageData *Page) (*Page, error) {
	offset := int64(pageID) * PageSize
	if offset >= p.fileSize {
		return pageData, fmt.Errorf("read past end of file: pageID %d, offset %d, fileSize %d", pageID, offset, p.fileSize)
	}
	_, err := p.file.ReadAt(pageData[:], offset)
	return pageData, err
}

func (p *Pager) WritePage(pageID PageID, pageData *Page) error {
	offset := int64(pageID) * PageSize
	_, err := p.file.WriteAt(pageData[:], offset)
	if err != nil {
		return err
	}

	// Update the file size and page count if we've written a new page
	// past the previous end of the file.
	if offset+PageSize > p.fileSize {
		p.fileSize = offset + PageSize
		p.numPages = p.fileSize / PageSize
	}

	return p.file.Sync()
}

func (p *Pager) AllocatePage() PageID {
	pageID := p.numPages
	p.numPages++
	p.fileSize += PageSize
	return PageID(pageID)
}

func (p *Pager) Close() error {
	return p.file.Close()
}
