/* Buffer Pool Manager requirements
1. Buffer Pool Manager has list of frames
2. Each frame has a page associated with it and its meta data
3. Buffer Pool Manager has only 1 DB file for now
4. Each DB file has multiple pages
5. Concurrency should be supported on the Buffer-Pool Manager (multiple should
be able to access the pool)
6. Ownership should be only movable as, concurrency should be supported
7. When frame is full, Eviction strategy should be supported [LRU for now, maybe
later based on access pattern]
8. When Page is not present in the pool, has to be pulled from Disk and loaded
in memory
9. Modified Pages to be written back to disk
*/
#pragma once
#include "../storage/Page.hpp"
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iosfwd>
#include <iostream>
#include <list>
#include <unordered_map>
#include <vector>

using frame_id_t = uint16_t;
constexpr frame_id_t INVALID_FRAME_ID = static_cast<frame_id_t>(-1);

class BufferPoolManager {

private:
  struct Frame {
    uint16_t page_id = INVALID_PAGE_ID;
    Page page;
    int pin_count;
    bool is_dirty;
  };

  std::size_t pool_size;                                // frames size
  std::unordered_map<page_id_t, frame_id_t> page_table; // page table
  std::vector<Frame> frames; // Fixed size Frames table
  std::list<frame_id_t> free_frames;
  std::list<frame_id_t> lru_list; // maintains access pattern
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator>
      lru_iterator; // keeps track of the iterator of lru_list
  std::fstream db_file;
  std::string db_file_name;

  //@ not default constructable and only movable
  BufferPoolManager() = default;
  BufferPoolManager(const BufferPoolManager &) = delete;
  BufferPoolManager &operator=(const BufferPoolManager &) = delete;

  bool readPageFromDisk(page_id_t page_id, Page *page) {

    if (!db_file.is_open()) {
      std::cerr << "Database file not open\n";
      return false;
    }

    // Page might be present in file or may not be
    // seek to the position where the page exists
    std::streampos offset = static_cast<std::streampos>(page_id) * PAGE_SIZE;
    db_file.seekg(offset);

    if (db_file.fail()) {
      // not present in file
      page->resetMemory();
      page->setPageId(page_id);
      return true;
    }

    db_file.read(page->getData(), PAGE_SIZE);
    if (db_file.gcount() != PAGE_SIZE) {
      page->resetMemory();
    }

    page->setPageId(page_id);
    db_file.clear();
    return true;
  }

  bool writePageToDisk(page_id_t page_id, Page *page) {

    if (!db_file.is_open()) {
      std::cerr << "Database file not open\n";
      return false;
    }
    // Page might be present in file or may not be
    // seek to the position where the page exists
    std::streampos offset = static_cast<std::streampos>(page_id) * PAGE_SIZE;
    db_file.seekp(offset);

    db_file.write(page->getData(), PAGE_SIZE);
    db_file.flush();

    if (db_file.bad()) {
      std::cerr << "Failed to write page " << page_id << " to disk\n";
      return false;
    }

    return true;
  }

  void updateLRU(frame_id_t frame_id) {
    if (lru_iterator.count(frame_id) > 0) {
      auto currIterator = lru_iterator[frame_id];
      lru_list.erase(currIterator);
    }
    // add it
    lru_list.push_back(frame_id);
    lru_iterator[frame_id] = std::prev(lru_list.end());
  }

  void removeFromLRU(frame_id_t frame_id) {
    if (lru_iterator.count(frame_id) > 0) {
      auto currIterator = lru_iterator[frame_id];
      lru_list.erase(currIterator);
      lru_iterator.erase(frame_id);
    }
  }

  bool evictPage() {
    frame_id_t evictFrameId = INVALID_FRAME_ID;
    for (auto frameId = lru_list.begin(); frameId != lru_list.end();
         frameId++) {
      Frame &frame = frames[*frameId];
      if (frame.pin_count == 0) {
        if (frame.is_dirty) {
          writePageToDisk(frame.page_id, &frame.page);
        }
        evictFrameId = *frameId;
        break;
      }
    }

    if (evictFrameId != INVALID_FRAME_ID) {
      // evict
      removeFromLRU(evictFrameId);
      page_table.erase(frames[evictFrameId].page_id);
      free_frames.push_back(evictFrameId);
      frames[evictFrameId].page_id = INVALID_PAGE_ID;
      return true;
    }

    return false;
  }

  void flushAllPages() {
    for (auto &frame : frames) {
      if (frame.page_id != INVALID_PAGE_ID && frame.is_dirty) {
        writePageToDisk(frame.page_id, &frame.page);
        frame.is_dirty = false;
      }
    }
  }
  static page_id_t pageIdCounter;

public:
  BufferPoolManager(const std::size_t poolSize, const std::string &fileName);

  Page *fetchPage(page_id_t page_id);

  bool unpinPage(page_id_t page_id, bool is_dirty);

  bool flushPage(page_id_t page_id);

  Page *newPage(page_id_t *page_id);

  bool deletePage(page_id_t page_id);

  void flushAllDirtyPages();

  ~BufferPoolManager(); // Destructor to flush and close file
};