#pragma once

#include <cstdint>
#include <cstring>

const int PAGE_SIZE = 4096; // 4KB Page size

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

public:
  Page();

  uint16_t getNumberOfRecords();

  void printStats();

  bool insertRecord(const char *data, uint16_t length);

  char *getRecord(uint16_t slot_num);

  bool updateRecord(uint16_t slot_num, char *data, int length);

  bool deleteRecord(uint16_t slot_num);

  bool writeToDisk(const char *fileName, uint32_t page_num);

  bool readFromDisk(const char *fileName, uint32_t page_num);
};