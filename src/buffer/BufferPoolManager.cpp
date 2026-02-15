#include "BufferPoolManager.hpp"
#include <fstream>
#include <ios>

page_id_t BufferPoolManager::pageIdCounter = 0;
BufferPoolManager::BufferPoolManager(const std::size_t poolSize,
                                     const std::string &fileName)
    : pool_size(poolSize), db_file_name(fileName) {

  // resize the frame
  frames.resize(pool_size);

  // clear the lists and maps
  free_frames.clear();
  page_table.clear();
  lru_list.clear();

  // free frames available
  for (int i = 0; i < pool_size; i++) {
    free_frames.push_back(i);
  }

  // open the DB file
  db_file.open(db_file_name, std::ios::binary | std::ios::in | std::ios::out);

  if (!db_file.is_open()) {
    // file does not exists
    db_file.clear(); // clear the flags if any set
    db_file.open(fileName, std::ios::binary | std::ios::trunc |
                               std::ios::out); // force creation
    db_file.close();
    db_file.open(db_file_name, std::ios::binary | std::ios::in | std::ios::out);
  }
}

BufferPoolManager::~BufferPoolManager() {
  if (db_file.is_open()) {
    flushAllPages();
    db_file.close();
  }

  // clear the lists and maps
  frames.clear();
  free_frames.clear();
  page_table.clear();
  lru_list.clear();
  lru_iterator.clear();
}

/*
1. checks page is in memory
2. If yes, increment pin_count, update lru_list and return page
3. If no, check if there is space in memory(pool)
4. If no, remove the item from beginning of lru_list
5. write the page to disk, remove it from the page_table, mark the frames
page_id as invalid
6. later, read the new requested page from disk, set the page_id in frame,
update the page_table, update lru_list, and return page
*/
Page *BufferPoolManager::fetchPage(page_id_t page_id) {

  std::cout << "Entered Page_id " << page_id << std::endl;
  if (page_table.count(page_id) > 0) {
    std::cout << "Page table has it" << std::endl;
    frames[page_table[page_id]].pin_count++;
    updateLRU(page_table[page_id]);
    return &frames[page_table[page_id]].page;
  }

  if (free_frames.empty() && !evictPage()) {
    std::cout << "could not evict" << std::endl;
    return nullptr;
  }

  if (free_frames.empty()) {
    std::cout << "Frames not empty" << std::endl;
    return nullptr;
  }

  frame_id_t availableFrameId = *free_frames.begin();
  free_frames.pop_front();

  // Load page from disk
  readPageFromDisk(page_id, &frames[availableFrameId].page);

  // Initialize frame
  frames[availableFrameId].page_id = page_id;
  frames[availableFrameId].pin_count = 1;
  frames[availableFrameId].is_dirty = false;

  // Update page table and LRU
  page_table[page_id] = availableFrameId;
  updateLRU(availableFrameId);

  return &frames[availableFrameId].page;
}

/*
1. checks page is in memory
2. Decrement the pin_count and set the is_dirty flag as requested
*/
bool BufferPoolManager::unpinPage(page_id_t page_id, bool is_dirty) {
  if (page_table.count(page_id) > 0) {
    if (frames[page_table[page_id]].pin_count <= 0) {
      return false;
    }

    frames[page_table[page_id]].pin_count--;

    if (is_dirty) {
      frames[page_table[page_id]].is_dirty = is_dirty;
    }

    return true;
  }
  return false;
}

/*
1. checks page is in memory
2. writes page to disk
*/
bool BufferPoolManager::flushPage(page_id_t page_id) {
  if (page_table.count(page_id) > 0) {
    // write only if no other thread is accessing and its dirty
    if (frames[page_table[page_id]].is_dirty) {
      bool success =
          writePageToDisk(page_id, &frames[page_table[page_id]].page);
      if (success) {
        frames[page_table[page_id]].is_dirty = false;
      }
      return success;
    }
    return true;
  }
  return false;
}
/*
Allocate new page_id, initialize empty page
*/
Page *BufferPoolManager::newPage(page_id_t *page_id) {

  if (free_frames.empty() && !evictPage()) {
    return nullptr;
  }

  if (free_frames.empty()) {
    return nullptr;
  }

  frame_id_t availableFrameId = *free_frames.begin();
  free_frames.pop_front();

  // allocate page id
  *page_id = pageIdCounter++;

  // update the frame
  frames[availableFrameId].page_id = *page_id;
  frames[availableFrameId].page.resetMemory();
  frames[availableFrameId].page.setPageId(*page_id);
  frames[availableFrameId].pin_count = 1;
  frames[availableFrameId].is_dirty = true;

  // update page table and LRU
  page_table[*page_id] = availableFrameId;
  updateLRU(availableFrameId);

  return &frames[availableFrameId].page;
}

/*
1. checks page is in memory
2. remove page from buffer
*/
bool BufferPoolManager::deletePage(page_id_t page_id) {
  if (page_table.count(page_id) > 0) {
    frame_id_t frameId = page_table[page_id];

    // no other thread is accessing it
    if (frames[frameId].pin_count == 0) {

      // if page is dirty
      if (frames[frameId].is_dirty) {
        writePageToDisk(page_id, &frames[frameId].page);
      }
      // update the frame
      frames[frameId].page_id = INVALID_PAGE_ID;
      frames[frameId].pin_count = 0;
      frames[frameId].is_dirty = false;

      // add it to free frames
      free_frames.push_back(frameId);

      // update page table and lru list
      page_table.erase(page_id);
      lru_list.erase(lru_iterator[frameId]);
      lru_iterator.erase(frameId);

      return true;
    }
  }
  return false;
}

/*
1. writes all dirty pages to disk
*/
void BufferPoolManager::flushAllDirtyPages() {
  for (auto &frame : frames) {
    if (frame.page_id != INVALID_PAGE_ID && frame.is_dirty) {
      writePageToDisk(frame.page_id, &frame.page);
      frame.is_dirty = false;
    }
  }
}
