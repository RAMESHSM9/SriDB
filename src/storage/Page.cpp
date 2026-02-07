#include "Page.hpp"
#include <fstream>
#include <ios>
#include <iostream>

Page::Page() {
  memset(buffer, 0, PAGE_SIZE);
  PageHeader *header = getHeader();
  header->num_of_slots = 0;
  header->free_space_start = sizeof(PageHeader);
  header->free_space_end = PAGE_SIZE;
}

bool Page::insertRecord(const char *data, uint16_t length) {

  // steps involved
  // 1. Find the offset to insert the data
  // 2. Insert the data using memcpy
  // 3. Add entry to Slot array
  // 4. update the header

  PageHeader *header = getHeader();
  // Slot *slot = getSlot(header->num_of_slots);

  uint16_t new_record_start = header->free_space_end - length;
  uint16_t slot_array_end =
      (sizeof(PageHeader) + (header->num_of_slots + 1) * sizeof(Slot));

  if (slot_array_end >= new_record_start) {
    return false;
  }

  // growing backward
  memcpy(buffer + new_record_start, data, length);

  // growing forward
  Slot *new_slot = getSlot(header->num_of_slots);
  new_slot->length = length;
  new_slot->offset = new_record_start;

  header->num_of_slots++;
  header->free_space_start = slot_array_end;
  header->free_space_end = new_record_start;

  return true;
}

char *Page::getRecord(uint16_t slot_num) {
  PageHeader *header = getHeader();
  if (slot_num >= header->num_of_slots) {
    std::cout << "No record exist for the given slot";
    return nullptr;
  }

  return (buffer + getSlot(slot_num)->offset);
}

uint16_t Page::getNumberOfRecords() { return getHeader()->num_of_slots; }

void Page::printStats() {
  PageHeader *header = getHeader();
  std::cout << "Page Stats:\n";
  std::cout << "  Num slots: " << header->num_of_slots << "\n";
  std::cout << "  Free space start: " << header->free_space_start << "\n";
  std::cout << "  Free space end: " << header->free_space_end << "\n";
  std::cout << "  Free space: "
            << (header->free_space_end - header->free_space_start)
            << " bytes\n";
}

bool Page::updateRecord(uint16_t slot_num, char *data, int length) {
  PageHeader *header = getHeader();
  if (slot_num >= header->num_of_slots) {
    std::cout << "Record does not exists" << std::endl;
    return false;
  }

  // get slot
  Slot *slot = getSlot(slot_num);

  if (slot->length < length) {
    std::cout << "Record failed to update because of size" << std::endl;
    return false;
  }

  // overwrite raw bytes at the offset
  memcpy(buffer + slot->offset, data, length);

  // no need to update slot/header, as its update operation
  return true;
}

bool Page::writeToDisk(const char *fileName, uint32_t page_num) {
  std::ofstream file(fileName, std::ios::binary | std::ios::in | std::ios::out);
  if (!file) {
    // file does exists already
    file.open(fileName, std::ios::binary | std::ios::out);
  }

  file.seekp(page_num * PAGE_SIZE);
  file.write(buffer, PAGE_SIZE);
  file.close();
  return true;
}

bool Page::readFromDisk(const char *fileName, uint32_t page_num) {
  std::ifstream file(fileName, std::ios::binary | std::ios::in);
  if (!file) {
    std::cout << "File Does not exists";
    return false;
  }
  file.seekg(page_num * PAGE_SIZE);
  file.read(buffer, PAGE_SIZE);
  return true;
}