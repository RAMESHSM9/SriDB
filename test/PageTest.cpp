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

// Test: Write and read from disk
TEST_F(PageTest, PersistenceToDisk) {
  User user1 = {1, "Alice", 25};
  User user2 = {2, "Bob", 30};

  page.insertRecord((char *)&user1, sizeof(User));
  page.insertRecord((char *)&user2, sizeof(User));

  // Write to disk
  const char *filename = "test_page.db";
  ASSERT_TRUE(page.writePageToDisk(filename, 0));

  // Create new page and read from disk
  Page page2;
  ASSERT_TRUE(page2.readPageFromDisk(filename, 0));

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

// Delete the record
/*TEST_F(PageTest, DeleteRecord) {
  User user = {1, "Alice", 25};

  ASSERT_TRUE(page.insertRecord(reinterpret_cast<char *>(&user), sizeof(user)));

  EXPECT_EQ(1, page.getNumberOfRecords());

  // slot is 0 based indexing
  ASSERT_TRUE(page.deleteRecord(0));

  EXPECT_EQ(0, page.getNumberOfRecords());
}*/

TEST_F(PageTest, DeleteRecord) {
  // Insert 3 records
  User user1 = {1, "Alice", 25};
  User user2 = {2, "Bob", 30};
  User user3 = {3, "Carol", 28};

  page.insertRecord((char *)&user1, sizeof(user1));
  page.insertRecord((char *)&user2, sizeof(user2));
  page.insertRecord((char *)&user3, sizeof(user3));

  // Delete the middle one
  bool success = page.deleteRecord(1);

  // What should be true after delete?
  ASSERT_TRUE(success);
  EXPECT_EQ(2, page.getNumberOfRecords());

  // try to get the deleted record
  char *deleted = page.getRecord(1);
  EXPECT_EQ(nullptr, deleted);

  // Can we still get the other records?
  User *record0 = (User *)page.getRecord(0);
  User *record2 = (User *)page.getRecord(2);
  EXPECT_EQ(1, record0->id);
  EXPECT_STREQ("Alice", record0->name);
  EXPECT_EQ(25, record0->age);

  EXPECT_EQ(3, record2->id);
  EXPECT_STREQ("Carol", record2->name);
  EXPECT_EQ(28, record2->age);
}

TEST_F(PageTest, DeleteAlreadyDeletedRecord) {
  User user = {1, "Alice", 25};
  page.insertRecord((char *)&user, sizeof(User));

  // Delete once - should succeed
  EXPECT_TRUE(page.deleteRecord(0));

  // Delete again - should fail
  EXPECT_FALSE(page.deleteRecord(0));
}

TEST_F(PageTest, CompactPageBasic) {
  User u1 = {1, "Alice", 25};
  User u2 = {2, "Bob", 30};
  User u3 = {3, "Carol", 28};

  page.insertRecord((char *)&u1, sizeof(User));
  page.insertRecord((char *)&u2, sizeof(User));
  page.insertRecord((char *)&u3, sizeof(User));

  std::cout << "Before delete:\n";
  page.printStats();

  // Delete middle record
  page.deleteRecord(1);

  std::cout << "\nAfter delete (before compact):\n";
  page.printStats();

  // Compact
  page.compactPage();

  std::cout << "\nAfter compact:\n";
  page.printStats();

  // Should have 2 records
  EXPECT_EQ(page.getNumberOfRecords(), 2);

  // Verify data integrity
  User *r0 = (User *)page.getRecord(0);
  User *r1 = (User *)page.getRecord(1);

  ASSERT_NE(r0, nullptr);
  ASSERT_NE(r1, nullptr);
  EXPECT_EQ(r0->id, 1); // Alice
  EXPECT_EQ(r1->id, 3); // Carol
  EXPECT_STREQ(r0->name, "Alice");
  EXPECT_STREQ(r1->name, "Carol");
}

TEST_F(PageTest, UpdateRecordGrow) {
  User u1 = {1, "Alice", 25};
  User u2 = {2, "Bob", 30};
  User u3 = {3, "Carol", 28};

  page.insertRecord((char *)&u1, sizeof(User));
  page.insertRecord((char *)&u2, sizeof(User));
  page.insertRecord((char *)&u3, sizeof(User));

  std::cout << "Before update:\n";
  page.printStats();

  // Make Bob's name longer (record grows)
  strcpy(u2.name, "Robert Anderson McKenzie");
  bool success = page.updateRecord(1, (char *)&u2, sizeof(User));

  std::cout << "\nAfter update (before compaction):\n";
  page.printStats();

  ASSERT_TRUE(success);
  EXPECT_EQ(page.getNumberOfRecords(), 3); // Still 3 active records

  // Bob should still be at slot 1!
  User *bob = (User *)page.getRecord(1);
  ASSERT_NE(bob, nullptr);
  EXPECT_EQ(bob->id, 2);
  EXPECT_STREQ(bob->name, "Robert Anderson McKenzie");

  // Other records still accessible
  User *alice = (User *)page.getRecord(0);
  User *carol = (User *)page.getRecord(2);
  EXPECT_EQ(alice->id, 1);
  EXPECT_EQ(carol->id, 3);

  // Now compact to clean up tombstone
  page.compactPage();

  std::cout << "\nAfter compaction:\n";
  page.printStats();

  // Should still have 3 records, but more free space
  EXPECT_EQ(page.getNumberOfRecords(), 3);
}

TEST_F(PageTest, UpdateRecordGrowNoSpace) {
  // Insert small users
  User user = {1, "Test", 25};
  int count = 0;
  while (page.insertRecord((char *)&user, sizeof(User))) {
    count++;
    user.id++;
  }

  std::cout << "Inserted " << count << " records\n";
  page.printStats();

  // Try to grow with a LARGER struct
  struct LargeUser {
    int id;
    char name[200]; // Much bigger!
    int age;
  } large_user = {1, "Very Long Name", 25};

  bool success = page.updateRecord(0, (char *)&large_user, sizeof(LargeUser));

  EXPECT_FALSE(success); // Should fail - sizeof(LargeUser) > sizeof(User)
}

TEST_F(PageTest, UpdateMultipleThenCompact) {
  User u1 = {1, "A", 25};
  User u2 = {2, "B", 30};
  User u3 = {3, "C", 28};

  page.insertRecord((char *)&u1, sizeof(User));
  page.insertRecord((char *)&u2, sizeof(User));
  page.insertRecord((char *)&u3, sizeof(User));

  // Update all three with longer names (creates 3 tombstones)
  strcpy(u1.name, "Alice Anderson");
  strcpy(u2.name, "Bob Baker");
  strcpy(u3.name, "Carol Cooper");

  page.updateRecord(0, (char *)&u1, sizeof(User));
  page.updateRecord(1, (char *)&u2, sizeof(User));
  page.updateRecord(2, (char *)&u3, sizeof(User));

  std::cout << "After 3 updates (3 tombstones):\n";
  page.printStats();

  // Compact should clean up all 3 tombstones
  page.compactPage();

  std::cout << "\nAfter compaction:\n";
  page.printStats();

  // All data should still be intact
  User *a = (User *)page.getRecord(0);
  User *b = (User *)page.getRecord(1);
  User *c = (User *)page.getRecord(2);

  EXPECT_STREQ(a->name, "Alice Anderson");
  EXPECT_STREQ(b->name, "Bob Baker");
  EXPECT_STREQ(c->name, "Carol Cooper");
}

TEST_F(PageTest, InsertSmartWithCompaction) {
  // Insert 5 records
  User users[5];
  for (int i = 0; i < 5; i++) {
    users[i] = {i, "User", 25};
    page.insertRecord((char *)&users[i], sizeof(User));
  }

  std::cout << "After 5 inserts:\n";
  page.printStats();

  // Delete 3 of them (create tombstones)
  page.deleteRecord(1);
  page.deleteRecord(2);
  page.deleteRecord(3);

  std::cout << "\nAfter 3 deletes (tombstones created):\n";
  page.printStats();
  std::cout << "Contiguous free: " << page.getContiguousFreeSpace() << "\n";
  std::cout << "Total free: " << page.getTotalFreeSpace() << "\n";

  // Fill remaining contiguous space
  User filler = {99, "Filler", 30};
  while (page.insertRecord((char *)&filler, sizeof(User))) {
    filler.id++;
  }

  std::cout << "\nAfter filling contiguous space:\n";
  page.printStats();

  // Normal insert should fail (no contiguous space)
  User new_user = {100, "New", 40};
  bool normal = page.insertRecord((char *)&new_user, sizeof(User));
  EXPECT_FALSE(normal);

  // Smart insert should succeed (compacts tombstones)
  bool smart = page.insertRecordSmart((char *)&new_user, sizeof(User));
  ASSERT_TRUE(smart);

  std::cout << "\nAfter smart insert:\n";
  page.printStats();

  // Verify the record was inserted
  // It should be at the last slot
  uint16_t last_slot = page.getNumberOfRecords() - 1;
  User *retrieved = (User *)page.getRecord(last_slot);
  EXPECT_EQ(retrieved->id, 100);
}

TEST_F(PageTest, InsertSmartStillFails) {
  // Fill page completely
  User user = {1, "Test", 25};
  while (page.insertRecordSmart((char *)&user, sizeof(User))) {
    user.id++;
  }

  std::cout << "Page completely full:\n";
  page.printStats();

  // Even smart insert should fail now
  bool success = page.insertRecordSmart((char *)&user, sizeof(User));
  EXPECT_FALSE(success);
}

TEST_F(PageTest, NeedsCompactionCheck) {
  // Insert 10 records
  User user = {1, "Test", 25};
  for (int i = 0; i < 10; i++) {
    page.insertRecord((char *)&user, sizeof(User));
    user.id++;
  }

  EXPECT_FALSE(page.needsCompaction());

  // Delete 3 (30% tombstones) - should trigger threshold
  page.deleteRecord(1);
  page.deleteRecord(3);
  page.deleteRecord(5);

  EXPECT_TRUE(page.needsCompaction());

  // Compact
  page.compactPage();

  EXPECT_FALSE(page.needsCompaction());
}