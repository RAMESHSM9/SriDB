#include "storage/Page.hpp"
#include <cstring>
#include <gtest/gtest.h>

// Simple test record structure
struct User {
  int id;
  char name[50];
  int age;
};

// Test fixture for Page tests
class PageTest : public ::testing::Test {
protected:
  Page page;

  void SetUp() override {
    // Runs before each test
  }

  void TearDown() override {
    // Runs after each test
  }
};

// Test: Can create an empty page
TEST_F(PageTest, CreateEmptyPage) { EXPECT_EQ(page.getNumberOfRecords(), 0); }

// Test: Can insert a single record
TEST_F(PageTest, InsertSingleRecord) {
  User user = {1, "Alice", 25};

  bool success = page.insertRecord((char *)&user, sizeof(User));

  ASSERT_TRUE(success);
  EXPECT_EQ(page.getNumberOfRecords(), 1);
}

// Test: Can retrieve inserted record
TEST_F(PageTest, InsertAndRetrieveRecord) {
  User user = {1, "Alice", 25};

  page.insertRecord((char *)&user, sizeof(User));

  char *record_data = page.getRecord(0);
  ASSERT_NE(record_data, nullptr);

  User *retrieved = (User *)record_data;
  EXPECT_EQ(retrieved->id, 1);
  EXPECT_STREQ(retrieved->name, "Alice");
  EXPECT_EQ(retrieved->age, 25);
}

// Test: Can insert multiple records
TEST_F(PageTest, InsertMultipleRecords) {
  User user1 = {1, "Alice", 25};
  User user2 = {2, "Bob", 30};
  User user3 = {3, "Carol", 28};

  ASSERT_TRUE(page.insertRecord((char *)&user1, sizeof(User)));
  ASSERT_TRUE(page.insertRecord((char *)&user2, sizeof(User)));
  ASSERT_TRUE(page.insertRecord((char *)&user3, sizeof(User)));

  EXPECT_EQ(page.getNumberOfRecords(), 3);

  // Verify all records
  User *retrieved1 = (User *)page.getRecord(0);
  User *retrieved2 = (User *)page.getRecord(1);
  User *retrieved3 = (User *)page.getRecord(2);

  EXPECT_EQ(retrieved1->id, 1);
  EXPECT_EQ(retrieved2->id, 2);
  EXPECT_EQ(retrieved3->id, 3);
}

// Test: Update record with same size
TEST_F(PageTest, UpdateRecordSameSize) {
  User user = {1, "Alice", 25};
  page.insertRecord((char *)&user, sizeof(User));

  // Update age
  user.age = 26;
  bool success = page.updateRecord(0, (char *)&user, sizeof(User));

  ASSERT_TRUE(success);

  User *retrieved = (User *)page.getRecord(0);
  EXPECT_EQ(retrieved->age, 26);
}

// Test: Update fails when new data is larger
TEST_F(PageTest, UpdateRecordLargerSizeFails) {
  User user = {1, "Alice", 25};
  page.insertRecord((char *)&user, sizeof(User));

  // Try to update with larger data
  char large_data[200];
  memset(large_data, 'X', 200);

  bool success = page.updateRecord(0, large_data, 200);

  EXPECT_FALSE(success); // Should fail
}

// Test: Write and read from disk
TEST_F(PageTest, PersistenceToDisk) {
  User user1 = {1, "Alice", 25};
  User user2 = {2, "Bob", 30};

  page.insertRecord((char *)&user1, sizeof(User));
  page.insertRecord((char *)&user2, sizeof(User));

  // Write to disk
  const char *filename = "test_page.db";
  ASSERT_TRUE(page.writeToDisk(filename, 0));

  // Create new page and read from disk
  Page page2;
  ASSERT_TRUE(page2.readFromDisk(filename, 0));

  // Verify data persisted
  EXPECT_EQ(page2.getNumberOfRecords(), 2);

  User *retrieved1 = (User *)page2.getRecord(0);
  User *retrieved2 = (User *)page2.getRecord(1);

  EXPECT_EQ(retrieved1->id, 1);
  EXPECT_STREQ(retrieved1->name, "Alice");
  EXPECT_EQ(retrieved2->id, 2);
  EXPECT_STREQ(retrieved2->name, "Bob");

  // Cleanup
  remove(filename);
}

// Test: Cannot insert when page is full
TEST_F(PageTest, PageFullness) {
  // Insert records until page is full
  User user = {1, "TestUser", 25};
  int count = 0;

  while (page.insertRecord((char *)&user, sizeof(User))) {
    count++;
    user.id++;
  }

  EXPECT_GT(count, 0); // Should have inserted at least some records

  // Try to insert one more - should fail
  bool success = page.insertRecord((char *)&user, sizeof(User));
  EXPECT_FALSE(success);
}

// Test: Get invalid slot returns nullptr
TEST_F(PageTest, GetInvalidSlot) {
  User user = {1, "Alice", 25};
  page.insertRecord((char *)&user, sizeof(User));

  // Try to get non-existent slot
  char *record = page.getRecord(999);
  EXPECT_EQ(record, nullptr);
}

// Test: Update invalid slot fails
TEST_F(PageTest, UpdateInvalidSlot) {
  User user = {1, "Alice", 25};

  // Try to update non-existent record
  bool success = page.updateRecord(0, (char *)&user, sizeof(User));
  EXPECT_FALSE(success);
}