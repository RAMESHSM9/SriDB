#include "Page.hpp"
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <ios>
#include <iostream>
#include <vector>

Page::Page() { resetMemory(); }

void Page::resetMemory() {
  memset(buffer, 0, PAGE_SIZE);
  PageHeader *header = getHeader();
  header->num_of_slots = 0;
  header->free_space_start = sizeof(PageHeader);
  header->free_space_end = PAGE_SIZE;
  page_id = INVALID_PAGE_ID;
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

bool Page::insertRecordSmart(char *data, uint16_t length) {

  if (insertRecord(data, length)) {
    return true;
  }

  uint16_t totalFreeSpace = getTotalFreeSpace();
  uint16_t neededSpace = length + sizeof(Slot);
  if (neededSpace > totalFreeSpace)
    return false;

  compactPage();
  return insertRecord(data, length);
}

char *Page::getRecord(uint16_t slot_num) {
  PageHeader *header = getHeader();

  if (slot_num >= header->num_of_slots) {
    std::cout << "No record exist for the given slot";
    return nullptr;
  }

  Slot *slot = getSlot(slot_num);
  if (slot->isDeleted) {
    std::cout << "No record exist for the given slot";
    return nullptr;
  }

  return (buffer + slot->offset);
}

bool Page::deleteRecord(uint16_t slot_num) {
  PageHeader *header = getHeader();
  if (slot_num >= header->num_of_slots) {
    std::cout << "No record exist for the given slot";
    return false;
  }

  Slot *slot = getSlot(slot_num);

  // whether it already deleted
  if (slot->isDeleted) {
    std::cout << "No record exist for the given slot";
    return false;
  }

  // mark the slot as deleted
  //  dont touch the record (will be claimed as part of compaction)
  slot->isDeleted = true;

  return true;
}

uint16_t Page::getNumberOfRecords() {
  int count = 0;
  for (int i = 0; i < getHeader()->num_of_slots; i++) {
    Slot *slot = getSlot(i);
    if (!slot->isDeleted) {
      count++;
    }
  }
  return count;
}

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

  // check whether the slot is deleted
  if (slot->isDeleted) {
    std::cout << "Record does not exists" << std::endl;
    return false;
  }

  if (slot->length >= length) {
    // overwrite raw bytes at the offset
    memcpy(buffer + slot->offset, data, length);
    return true;
  }

  // check for space
  uint16_t new_free_space_start = header->free_space_end - length;
  uint16_t new_slot_offset =
      sizeof(PageHeader) + (header->num_of_slots + 1) * sizeof(Slot);

  if (new_slot_offset >= new_free_space_start) {
    return false;
  }

  // get new slot, mark it as deleted and the compact
  Slot *tombStoneSlot = getSlot(header->num_of_slots);
  header->num_of_slots++;
  *tombStoneSlot = *slot;
  tombStoneSlot->isDeleted = true;

  memcpy(buffer + new_free_space_start, data, length);

  // update slot
  slot->offset = new_free_space_start;
  slot->length = length;

  header->free_space_end = new_free_space_start;

  return true;
}

bool Page::writePageToDisk(const char *fileName, uint32_t page_num) {
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

bool Page::readPageFromDisk(const char *fileName, uint32_t page_num) {
  std::ifstream file(fileName, std::ios::binary | std::ios::in);
  if (!file) {
    std::cout << "File Does not exists";
    return false;
  }
  file.seekg(page_num * PAGE_SIZE);
  file.read(buffer, PAGE_SIZE);
  return true;
}

void Page::compactPage() {
  PageHeader *header = getHeader();

  std::vector<std::pair<uint16_t, Slot *>> slotArray;

  for (int i = 0; i < header->num_of_slots; i++) {
    Slot *slot = getSlot(i);
    slotArray.push_back(std::make_pair(slot->offset, slot));
  }

  // sort the slotArray (from highest to lowest)
  std::sort(slotArray.begin(), slotArray.end(),
            [](std::pair<uint16_t, Slot *> &A, std::pair<uint16_t, Slot *> &B) {
              return (A.first > B.first);
            });

  int cummulative_gap = 0;
  uint16_t lastOffset = PAGE_SIZE;
  for (auto &slot : slotArray) {
    if (slot.second->isDeleted) {
      cummulative_gap += slot.second->length;
    } else {
      // slot which is not deleted
      uint16_t newSlotOffset = slot.second->offset + cummulative_gap;
      memmove(buffer + newSlotOffset, buffer + slot.second->offset,
              slot.second->length);
      slot.second->offset = newSlotOffset;
      lastOffset = newSlotOffset;
    }
  }

  // move slots
  header->num_of_slots = 0;
  for (auto slot : slotArray) {
    if (!slot.second->isDeleted) {
      Slot *newSlot = getSlot(header->num_of_slots);
      *newSlot = *(slot.second);
      header->num_of_slots++;
    }
  }

  // update header
  header->free_space_start =
      sizeof(PageHeader) + (header->num_of_slots * sizeof(Slot));
  header->free_space_end = lastOffset;
}

uint16_t Page::getContiguousFreeSpace() {
  PageHeader *header = getHeader();
  return (header->free_space_end - header->free_space_end);
}

uint16_t Page::getTotalFreeSpace() {
  PageHeader *header = getHeader();
  uint16_t total = (header->free_space_end - header->free_space_end);

  for (uint16_t i = 0; i < header->num_of_slots; i++) {
    Slot *slot = getSlot(i);
    if (slot->isDeleted) {
      total += slot->length;
    }
  }

  return total;
}

bool Page::needsCompaction() {
  PageHeader *header = getHeader();
  if (header->num_of_slots == 0) {
    return false;
  }

  uint16_t tombstone_count = 0;
  for (uint16_t i = 0; i < header->num_of_slots; i++) {
    if (getSlot(i)->isDeleted) {
      tombstone_count++;
    }
  }
  // Compact if >25% of slots are tombstones
  return tombstone_count > (header->num_of_slots / 4);
}
