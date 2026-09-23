"""Negative controls for the MW oracle and evidence pipeline."""
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from pymongo.errors import NetworkTimeout, WriteConcernError, WTimeoutError

from analysis.analyze import summarize
from experiments.common import JsonlWriter, execute_operation
from experiments.test_monotonic_writes import classify_trial, history_entry


def ordered_evidence():
    w1 = {'status': 'success', 'operation_id': 'trial:write_1',
          'returned_document': {'version': 0, 'client_seq': 0, 'history': []}}
    h1 = history_entry('trial:write_1', 1)
    h2 = history_entry('trial:write_2', 2)
    w2 = {'status': 'success', 'operation_id': 'trial:write_2',
          'returned_document': {'version': 1, 'client_seq': 1, 'history': [h1]}}
    audit = {'status': 'success', 'returned_document': {
        'version': 2, 'client_seq': 2, 'history': [h1, h2]}}
    return w1, w2, audit


class OracleTests(unittest.TestCase):
    def test_ordered_history(self):
        self.assertEqual(classify_trial(*ordered_evidence())[0], 'no_violation')

    def test_final_version_two_does_not_hide_missing_predecessor(self):
        w1, w2, audit = ordered_evidence()
        w2['returned_document']['history'] = []
        self.assertEqual(classify_trial(w1, w2, audit)[0], 'candidate_violation')

    def test_wrong_writer_history_not_accepted(self):
        w1, w2, audit = ordered_evidence()
        w2['returned_document']['history'][0]['writer'] = 'client-B'
        self.assertEqual(classify_trial(w1, w2, audit)[0], 'candidate_violation')

    def test_reversed_final_history(self):
        w1, w2, audit = ordered_evidence()
        audit['returned_document']['history'].reverse()
        self.assertEqual(classify_trial(w1, w2, audit)[0], 'candidate_violation')

    def test_timeout_is_not_a_violation_or_pass(self):
        for write in (0, 1):
            evidence = ordered_evidence()
            evidence[write]['status'] = 'timeout'
            self.assertEqual(classify_trial(*evidence)[0], 'inconclusive')

    def test_missing_document_excludes_noop(self):
        w1, w2, audit = ordered_evidence()
        w2['returned_document'] = None
        self.assertEqual(classify_trial(w1, w2, audit)[0], 'inconclusive')

    def test_bad_initial_state(self):
        w1, w2, audit = ordered_evidence()
        w1['returned_document']['version'] = 4
        self.assertEqual(classify_trial(w1, w2, audit)[0], 'invalid_trial')

    def test_audit_failure_does_not_pass(self):
        w1, w2, _ = ordered_evidence()
        self.assertEqual(classify_trial(w1, w2, {'status': 'failure'})[0], 'inconclusive')


class LoggingTests(unittest.TestCase):
    def test_write_timeout_unknown_effect_is_retained(self):
        self.check_error(NetworkTimeout('ambiguous acknowledgement'), 'timeout')

    def test_write_concern_error_not_success(self):
        self.check_error(WriteConcernError('not replicated', 64), 'failure')

    def test_write_concern_timeout_unknown_effect(self):
        self.check_error(WTimeoutError('replication timeout', 64), 'timeout')

    def check_error(self, error, expected):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'operations.jsonl'
            writer = JsonlWriter(path)
            client = SimpleNamespace(topology_description=SimpleNamespace(
                server_descriptions=lambda: {}))
            def fail():
                raise error
            row = execute_operation(client, writer, {'trial_id': 'test'}, 'write_1',
                                    fail, written_version=1)
            writer.close()
            self.assertEqual(row['status'], expected)
            self.assertEqual(row['write_effect'], 'unknown')
            self.assertIsNone(row['is_violation'])
            self.assertIsNone(row['target_node'])
            self.assertEqual(json.loads(path.read_text())['error'], str(error))

    def test_summary_keeps_timeouts_out_of_success_latency(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'operations.jsonl'
            op = {'record_type': 'operation', 'config_id': 'C1', 'trial_id': 'a',
                  'phase': 'workload', 'status': 'success', 'latency_ms': 1, 'retry_count': 0}
            timeout = {**op, 'trial_id': 'b', 'status': 'timeout', 'latency_ms': 10000}
            trial = {'record_type': 'trial_result', 'config_id': 'C1', 'trial_id': 'a',
                     'classification': 'no_violation', 'is_violation': False,
                     'rollback': 'not_assessed', 'observation_stale': False}
            path.write_text('\n'.join(json.dumps(r) for r in (op, timeout, trial)) + '\n')
            row = summarize(path)[0]
            self.assertEqual(row['incomplete_trials'], 1)
            self.assertEqual(row['write_timeout'], 1)
            self.assertEqual(row['write_latency_p95_ms'], 1)
            self.assertEqual(row['confirmed_violation'], 0)


if __name__ == '__main__':
    unittest.main()
