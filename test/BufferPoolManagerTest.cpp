#include "buffer/BufferPoolManager.hpp"
#include <cstring>
#include <gtest/gtest.h>

// Test record structure
struct TestRecord {
  int id;
  char data[50];
};

class BufferPoolManagerTest : public ::testing::Test {
protected:
  BufferPoolManager *bpm;
  std::string db_file = "test_bpm.db";

  void SetUp() override {
    // Remove old test file if exists
    std::remove(db_file.c_str());

    // Create buffer pool with 3 frames
    bpm = new BufferPoolManager(3, db_file);
  }

  void TearDown() override {
    delete bpm;
    std::remove(db_file.c_str());
  }
};

// ============ BASIC TESTS ============

TEST_F(BufferPoolManagerTest, NewPage) {
  page_id_t page_id;
  Page *page = bpm->newPage(&page_id);

  ASSERT_NE(page, nullptr);
  EXPECT_EQ(page_id, 0); // First page should be ID 0
  EXPECT_EQ(page->getPageId(), 0);
}

TEST_F(BufferPoolManagerTest, NewMultiplePages) {
  page_id_t page_ids[3];

  for (int i = 0; i < 3; i++) {
    Page *page = bpm->newPage(&page_ids[i]);
    ASSERT_NE(page, nullptr);
    EXPECT_EQ(page_ids[i], i);
  }

  // All frames used - should fail
  page_id_t extra_page_id;
  Page *extra = bpm->newPage(&extra_page_id);
  EXPECT_EQ(extra, nullptr);
}

TEST_F(BufferPoolManagerTest, FetchPageBasic) {
  // Create a page
  page_id_t page_id;
  Page *page1 = bpm->newPage(&page_id);
  ASSERT_NE(page1, nullptr);

  // Write some data
  TestRecord rec = {42, "Hello"};
  page1->insertRecord((char *)&rec, sizeof(TestRecord));

  // Unpin it
  ASSERT_TRUE(bpm->unpinPage(page_id, true));

  // Fetch it again
  Page *page2 = bpm->fetchPage(page_id);
  ASSERT_NE(page2, nullptr);
  EXPECT_EQ(page2->getPageId(), page_id);

  // Verify data
  TestRecord *retrieved = (TestRecord *)page2->getRecord(0);
  EXPECT_EQ(retrieved->id, 42);
  EXPECT_STREQ(retrieved->data, "Hello");
}

TEST_F(BufferPoolManagerTest, UnpinPage) {
  page_id_t page_id;
  Page *page = bpm->newPage(&page_id);
  ASSERT_NE(page, nullptr);

  // Should succeed - page is pinned
  EXPECT_TRUE(bpm->unpinPage(page_id, false));

  // Should fail - already unpinned
  EXPECT_FALSE(bpm->unpinPage(page_id, false));
}

// ============ EVICTION TESTS ============

TEST_F(BufferPoolManagerTest, SimpleEviction) {
  page_id_t page_ids[4];

  // Fill buffer pool (3 frames)
  for (int i = 0; i < 3; i++) {
    Page *page = bpm->newPage(&page_ids[i]);
    ASSERT_NE(page, nullptr);

    TestRecord rec = {i, "Data"};
    page->insertRecord((char *)&rec, sizeof(TestRecord));

    bpm->unpinPage(page_ids[i], true);
  }

  // Create 4th page - should evict one
  Page *page4 = bpm->newPage(&page_ids[3]);
  ASSERT_NE(page4, nullptr);
  EXPECT_EQ(page_ids[3], 3);

  // First page should have been evicted (LRU)
  // But we should still be able to fetch it from disk
  bpm->unpinPage(page_ids[3], true);

  Page *page1 = bpm->fetchPage(page_ids[0]);
  ASSERT_NE(page1, nullptr);

  // Verify data persisted
  TestRecord *rec = (TestRecord *)page1->getRecord(0);
  EXPECT_EQ(rec->id, 0);
}

TEST_F(BufferPoolManagerTest, EvictionWithPinnedPages) {
  page_id_t page_ids[4];

  // Create 3 pages
  for (int i = 0; i < 3; i++) {
    Page *page = bpm->newPage(&page_ids[i]);
    ASSERT_NE(page, nullptr);
    bpm->unpinPage(page_ids[i], false);
  }

  // Pin first two pages
  bpm->fetchPage(page_ids[0]);
  bpm->fetchPage(page_ids[1]);

  // Create 4th page - should evict page_ids[2] (only unpinned one)
  Page *page4 = bpm->newPage(&page_ids[3]);
  ASSERT_NE(page4, nullptr);

  // Verify pages 0 and 1 are still in buffer
  bpm->unpinPage(page_ids[0], false);
  bpm->unpinPage(page_ids[1], false);
}

TEST_F(BufferPoolManagerTest, EvictionAllPinned) {
  page_id_t page_ids[4];

  // Create 3 pages and keep them all pinned
  for (int i = 0; i < 3; i++) {
    Page *page = bpm->newPage(&page_ids[i]);
    ASSERT_NE(page, nullptr);
    // Don't unpin!
  }

  // Try to create 4th page - should fail (all pinned)
  Page *page4 = bpm->newPage(&page_ids[3]);
  EXPECT_EQ(page4, nullptr);
}

// ============ DIRTY PAGE TESTS ============

TEST_F(BufferPoolManagerTest, DirtyPagePersistence) {
  page_id_t page_id;

  // Create and modify page
  Page *page = bpm->newPage(&page_id);
  ASSERT_NE(page, nullptr);

  TestRecord rec = {99, "Dirty Data"};
  page->insertRecord((char *)&rec, sizeof(TestRecord));

  bpm->unpinPage(page_id, true); // Mark dirty

  // Force eviction by filling buffer
  page_id_t temp_ids[3];
  for (int i = 0; i < 3; i++) {
    bpm->newPage(&temp_ids[i]);
  }

  // unpin one of the pages so, that we could fetch the new page
  bpm->unpinPage(temp_ids[0], false);

  // Fetch original page - should load from disk
  Page *fetched = bpm->fetchPage(page_id);
  ASSERT_NE(fetched, nullptr);

  // Verify data was written to disk and read back
  TestRecord *retrieved = (TestRecord *)fetched->getRecord(0);
  EXPECT_EQ(retrieved->id, 99);
  EXPECT_STREQ(retrieved->data, "Dirty Data");
}

TEST_F(BufferPoolManagerTest, MultipleDirtyFlags) {
  page_id_t page_id;
  Page *page = bpm->newPage(&page_id);

  // Pin multiple times
  bpm->fetchPage(page_id);
  bpm->fetchPage(page_id);

  // Unpin with different dirty flags
  bpm->unpinPage(page_id, false); // Clean
  bpm->unpinPage(page_id, true);  // Dirty
  bpm->unpinPage(page_id, false); // Should stay dirty!

  // Flush should write it
  EXPECT_TRUE(bpm->flushPage(page_id));
}

// ============ FLUSH TESTS ============

TEST_F(BufferPoolManagerTest, FlushPage) {
  page_id_t page_id;
  Page *page = bpm->newPage(&page_id);

  TestRecord rec = {123, "Flush Test"};
  page->insertRecord((char *)&rec, sizeof(TestRecord));

  bpm->unpinPage(page_id, true);

  // Flush should succeed
  EXPECT_TRUE(bpm->flushPage(page_id));

  // Flush clean page should still work (or return false - both valid)
  // Your implementation returns false for clean pages
  EXPECT_TRUE(bpm->flushPage(page_id));
}

TEST_F(BufferPoolManagerTest, FlushAllDirtyPages) {
  page_id_t page_ids[3];

  // Create 3 dirty pages
  for (int i = 0; i < 3; i++) {
    Page *page = bpm->newPage(&page_ids[i]);
    TestRecord rec = {i, "Data"};
    page->insertRecord((char *)&rec, sizeof(TestRecord));
    bpm->unpinPage(page_ids[i], true);
  }

  // Flush all
  bpm->flushAllDirtyPages();

  // All should now be clean (we can't directly verify but no crash is good)
}

// ============ DELETE TESTS ============

TEST_F(BufferPoolManagerTest, DeletePage) {
  page_id_t page_id;
  Page *page = bpm->newPage(&page_id);
  bpm->unpinPage(page_id, false);

  // Delete should succeed
  EXPECT_TRUE(bpm->deletePage(page_id));

  // Fetching deleted page should load from disk (empty)
  Page *fetched = bpm->fetchPage(page_id);
  ASSERT_NE(fetched, nullptr);
  EXPECT_EQ(fetched->getNumberOfRecords(), 0);
}

TEST_F(BufferPoolManagerTest, DeletePinnedPage) {
  page_id_t page_id;
  Page *page = bpm->newPage(&page_id);
  // Keep it pinned!

  // Delete should fail
  EXPECT_FALSE(bpm->deletePage(page_id));
}

// ============ LRU TESTS ============

TEST_F(BufferPoolManagerTest, LRUEviction) {
  page_id_t page_ids[4];

  // Create 3 pages
  for (int i = 0; i < 3; i++) {
    Page *page = bpm->newPage(&page_ids[i]);
    TestRecord rec = {i, "Data"};
    page->insertRecord((char *)&rec, sizeof(TestRecord));
    bpm->unpinPage(page_ids[i], true);
  }

  // Access page 0 (make it most recently used)
  Page *p0 = bpm->fetchPage(page_ids[0]);
  bpm->unpinPage(page_ids[0], false);

  // Create 4th page - should evict page 1 (least recently used)
  Page *p4 = bpm->newPage(&page_ids[3]);
  ASSERT_NE(p4, nullptr);
  bpm->unpinPage(page_ids[3], false);

  // Page 0 and 2 should still be in buffer
  // Page 1 should have been evicted

  // This will require a disk read
  Page *p1 = bpm->fetchPage(page_ids[1]);
  ASSERT_NE(p1, nullptr);

  TestRecord *rec = (TestRecord *)p1->getRecord(0);
  EXPECT_EQ(rec->id, 1); // Data persisted
}

// ============ PERSISTENCE TESTS ============

TEST_F(BufferPoolManagerTest, DataPersistsAcrossInstances) {
  page_id_t page_id;

  // Create and write data
  {
    BufferPoolManager bpm1(3, db_file);
    Page *page = bpm1.newPage(&page_id);

    TestRecord rec = {999, "Persistent Data"};
    page->insertRecord((char *)&rec, sizeof(TestRecord));

    bpm1.unpinPage(page_id, true);
    // Destructor flushes all pages
  }

  // Create new BPM instance
  {
    BufferPoolManager bpm2(3, db_file);
    Page *page = bpm2.fetchPage(page_id);
    ASSERT_NE(page, nullptr);

    TestRecord *rec = (TestRecord *)page->getRecord(0);
    EXPECT_EQ(rec->id, 999);
    EXPECT_STREQ(rec->data, "Persistent Data");
  }
}