#pragma once

#include <cstdint>
#include <cstring>

const int PAGE_SIZE = 4096; // 4KB Page size
using page_id_t = uint16_t;
static constexpr page_id_t INVALID_PAGE_ID = static_cast<page_id_t>(-1);
// 4KB Page
class Page {
private:
  struct PageHeader {
    uint16_t num_of_slots;     // indicates number of records in the Page
    uint16_t free_space_start; // free space start  (grows forward)
    uint16_t free_space_end;   // free space start (grows backward)
  };

  struct Slot {
    uint16_t offset; // start of the record
    uint16_t length; // record length
    bool isDeleted;  // flag to indicate that this slot is deleted
  };

  char buffer[PAGE_SIZE];

  PageHeader *getHeader() { return (PageHeader *)(buffer); }

  Slot *getSlot(uint16_t slot_num) {
    return (Slot *)(buffer + sizeof(PageHeader) + slot_num * sizeof(Slot));
  }

  page_id_t page_id = INVALID_PAGE_ID;

public:
  Page();

  uint16_t getNumberOfRecords();

  void printStats();

  bool insertRecord(const char *data, uint16_t length);

  char *getRecord(uint16_t slot_num);

  bool updateRecord(uint16_t slot_num, char *data, int length);

  bool deleteRecord(uint16_t slot_num);

  bool writePageToDisk(const char *fileName, uint32_t page_num);

  bool readPageFromDisk(const char *fileName, uint32_t page_num);

  void compactPage();

  bool insertRecordSmart(char *data, uint16_t length);

  uint16_t getContiguousFreeSpace();

  uint16_t getTotalFreeSpace();

  bool needsCompaction();

  page_id_t getPageId() { return page_id; }

  void setPageId(const page_id_t pageId) { page_id = pageId; }

  // Add these for BufferPoolManager access
  char *getData() { return buffer; }
  const char *getData() const { return buffer; }

  // Reset page to initial state (useful for newPage)
  void resetMemory();
};