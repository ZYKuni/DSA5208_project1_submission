"""Unit tests for conservative MR fault-result classification."""
import unittest

from experiments.run_mr_faults import classify


def read(status="success", version=2):
    return {"status": status, "returned_version": version}


class MrFaultClassificationTests(unittest.TestCase):
    def test_regression_is_violation(self):
        self.assertEqual(classify(read(version=2), read(version=1))[:2],
                         ("violation", True))

    def test_disappearance_is_violation(self):
        self.assertEqual(classify(read(version=2), read(version=None))[:2],
                         ("violation", True))

    def test_equal_or_newer_is_not_violation(self):
        for pair in ((1, 1), (1, 2), (2, 2)):
            with self.subTest(pair=pair):
                self.assertEqual(classify(read(version=pair[0]), read(version=pair[1]))[:2],
                                 ("no_violation", False))

    def test_availability_failure_is_inconclusive(self):
        self.assertEqual(classify(read(version=2), read(status="timeout"))[:2],
                         ("inconclusive", None))

    def test_missing_first_read_is_invalid(self):
        self.assertEqual(classify(read(version=None), read(version=1))[:2],
                         ("invalid_trial", None))


if __name__ == "__main__":
    unittest.main()
