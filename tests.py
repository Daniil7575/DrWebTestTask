import unittest
from contextlib import redirect_stdout
from io import StringIO

from db import Database


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.db = Database()

    def test_set_and_get(self):
        self.db.set_command(["key", "value"])
        with redirect_stdout(StringIO()) as f:
            self.db.get_command(["key"])
        self.assertEqual(f.getvalue().strip(), "value")

    def test_unset(self):
        self.db.set_command(["key", "value"])
        self.db.unset_command(["key"])
        with redirect_stdout(StringIO()) as f:
            self.db.get_command(["key"])
        self.assertEqual(f.getvalue().strip(), self.db.NULL_VALUE)

    def test_counts(self):
        self.db.set_command(["key1", "value"])
        self.db.set_command(["key2", "value"])
        self.db.set_command(["key3", "other"])
        with redirect_stdout(StringIO()) as f:
            self.db.counts_command(["value"])
        self.assertEqual(f.getvalue().strip(), "2")

    def test_find(self):
        self.db.set_command(["key1", "value"])
        self.db.set_command(["key2", "value"])
        self.db.set_command(["key3", "other"])
        with redirect_stdout(StringIO()) as f:
            self.db.find_command(["value"])
        self.assertEqual(f.getvalue().strip(), "key1 key2")

    def test_find_no_keys(self):
        with redirect_stdout(StringIO()) as f:
            self.db.find_command(["value"])
        self.assertEqual(f.getvalue().strip(), self.db.NULL_VALUE)

    # Тесты транзакций
    def test_transaction_rollback(self):
        self.db.set_command(["key", "value1"])
        self.db.begin_command([])
        self.db.set_command(["key", "value2"])
        with redirect_stdout(StringIO()) as f:
            self.db.get_command(["key"])
        self.assertEqual(f.getvalue().strip(), "value2")
        self.db.rollback_command([])
        with redirect_stdout(StringIO()) as f:
            self.db.get_command(["key"])
        self.assertEqual(f.getvalue().strip(), "value1")

    def test_transaction_commit(self):
        self.db.set_command(["key", "value1"])
        self.db.begin_command([])
        self.db.set_command(["key", "value2"])
        self.db.commit_command([])
        with redirect_stdout(StringIO()) as f:
            self.db.get_command(["key"])
        self.assertEqual(f.getvalue().strip(), "value2")

    def test_nested_transactions(self):
        self.db.set_command(["key", "value1"])
        self.db.begin_command([])
        self.db.set_command(["key", "value2"])
        self.db.begin_command([])
        self.db.set_command(["key", "value3"])
        with redirect_stdout(StringIO()) as f:
            self.db.get_command(["key"])
        self.assertEqual(f.getvalue().strip(), "value3")
        self.db.rollback_command([])
        with redirect_stdout(StringIO()) as f:
            self.db.get_command(["key"])
        self.assertEqual(f.getvalue().strip(), "value2")
        self.db.commit_command([])
        with redirect_stdout(StringIO()) as f:
            self.db.get_command(["key"])
        self.assertEqual(f.getvalue().strip(), "value2")

    def test_rollback_without_transaction(self):
        self.db.rollback_command([])
        self.assertEqual(len(self.db.stack), 1)

    def test_commit_without_transaction(self):
        self.db.commit_command([])
        self.assertEqual(len(self.db.stack), 1)

    def test_commit_reduces_stack(self):
        self.db.begin_command([])
        self.db.set_command(["key", "value"])
        self.db.commit_command([])
        self.assertEqual(len(self.db.stack), 1)
        self.assertEqual(self.db.stack[0].get("key"), "value")

    def test_rollback_reduces_stack(self):
        self.db.begin_command([])
        self.db.set_command(["key", "value"])
        self.db.rollback_command([])
        self.assertEqual(len(self.db.stack), 1)
        self.assertNotIn("key", self.db.stack[0])

    def test_get_value_with_unset_in_transaction(self):
        self.db.set_command(["key", "value"])
        self.db.begin_command([])
        self.db.unset_command(["key"])
        with redirect_stdout(StringIO()) as f:
            self.db.get_command(["key"])
        self.assertEqual(f.getvalue().strip(), self.db.NULL_VALUE)
        self.db.rollback_command([])
        with redirect_stdout(StringIO()) as f:
            self.db.get_command(["key"])
        self.assertEqual(f.getvalue().strip(), "value")
    
    def test_get_unsetted_value_after_commit(self):
        self.db.set_command(["key", "value"])
        self.db.begin_command([])
        self.db.unset_command(["key"])
        self.db.commit_command([])
        with redirect_stdout(StringIO()) as f:
            self.db.get_command(["key"])
        self.assertEqual(f.getvalue().strip(), self.db.NULL_VALUE)

if __name__ == "__main__":
    unittest.main()
