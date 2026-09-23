"""Unit tests for conservative RYW fault-result classification."""
import unittest

from experiments.run_ryw_faults import classify


class RywFaultClassificationTests(unittest.TestCase):
    def test_acknowledged_write_followed_by_older_version_is_violation(self):
        classification, violation, _ = classify(
            {"status": "success"},
            {"status": "success", "returned_version": 0},
        )
        self.assertEqual(classification, "violation")
        self.assertIs(violation, True)

    def test_missing_document_after_acknowledged_write_is_violation(self):
        classification, violation, _ = classify(
            {"status": "success"},
            {"status": "success", "returned_version": None},
        )
        self.assertEqual(classification, "violation")
        self.assertIs(violation, True)

    def test_visible_version_is_not_violation(self):
        classification, violation, _ = classify(
            {"status": "success"},
            {"status": "success", "returned_version": 1},
        )
        self.assertEqual(classification, "no_violation")
        self.assertIs(violation, False)

    def test_unacknowledged_write_is_inconclusive(self):
        classification, violation, _ = classify(
            {"status": "failure"}, None,
        )
        self.assertEqual(classification, "inconclusive")
        self.assertIsNone(violation)

    def test_read_timeout_is_inconclusive(self):
        classification, violation, _ = classify(
            {"status": "success"},
            {"status": "timeout", "returned_version": None},
        )
        self.assertEqual(classification, "inconclusive")
        self.assertIsNone(violation)


if __name__ == "__main__":
    unittest.main()
