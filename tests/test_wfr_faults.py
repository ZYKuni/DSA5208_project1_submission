"""Synthetic evidence and mocked orchestration; these are not MongoDB results."""
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from experiments import run_wfr_faults as wfr
from experiments.configs import get_config


def evidence(missing=False):
    read = {'status': 'success', 'target_node': 'mongo1:27017',
            'returned_document': {'_id': 't', 'x_version': 1, 'x_history': [0, 1]}}
    write = {'status': 'success', 'target_node': 'mongo2:27017', 'operation_id': 't:dependent_write',
             'returned_document': {'_id': 't', 'x_version': 0 if missing else 1,
                                   'x_history': [0] if missing else [0, 1]}}
    audit = {'status': 'success', 'returned_document': {
        '_id': 't', 'x_version': 0 if missing else 1, 'x_history': [0] if missing else [0, 1],
        'y': {'id': 't:dependent_write', 'depends_on_version': 1}}}
    return read, write, audit


class WfrClassificationTests(unittest.TestCase):
    def test_atomic_history_preserved(self):
        self.assertEqual(wfr.classify(*evidence())[:2], ('no_violation', False))

    def test_missing_history_is_candidate_not_confirmed(self):
        self.assertEqual(wfr.classify(*evidence(True))[:2], ('candidate_violation', None))

    def test_write_timeout_even_with_visible_marker_is_not_violation(self):
        rows = evidence(True)
        rows[1]['status'] = 'timeout'
        self.assertEqual(wfr.classify(*rows)[:2], ('inconclusive', None))
        self.assertEqual(wfr.availability(rows[1]), 'timeout')

    def test_read_timeout(self):
        rows = evidence()
        rows[0]['status'] = 'timeout'
        self.assertEqual(wfr.classify(*rows)[:2], ('inconclusive', None))

    def test_invalid_precondition(self):
        self.assertEqual(wfr.classify(*evidence(), invalid='x=0')[:2], ('invalid_trial', None))

    def test_final_read_alone_cannot_replace_atomic_preimage(self):
        rows = evidence(True)
        rows[1]['returned_document'] = None
        self.assertEqual(wfr.classify(*rows)[:2], ('inconclusive', None))

    def test_write_concern_error_is_not_acknowledgement(self):
        rows = evidence(True)
        rows[1]['attempts'] = [{'write_concern_error': {'code': 64}}]
        self.assertEqual(wfr.classify(*rows)[:2], ('inconclusive', None))

    def test_missing_audit(self):
        read, write, _ = evidence()
        self.assertEqual(wfr.classify(read, write, None)[:2], ('inconclusive', None))

    def test_unrelated_audit_marker(self):
        rows = evidence()
        rows[2]['returned_document']['y']['id'] = 'another-write'
        self.assertEqual(wfr.classify(*rows)[:2], ('inconclusive', None))

    def test_unrelated_document(self):
        for index in (1, 2):
            rows = evidence()
            rows[index]['returned_document']['_id'] = 'unrelated'
            self.assertEqual(wfr.classify(*rows)[:2], ('inconclusive', None))

    def test_malformed_history(self):
        rows = evidence()
        rows[0]['returned_document']['x_history'] = None
        self.assertEqual(wfr.classify(*rows)[:2], ('inconclusive', None))

    def test_missing_destination(self):
        rows = evidence()
        rows[1]['target_node'] = None
        self.assertEqual(wfr.classify(*rows)[:2], ('inconclusive', None))

    def test_causal_session_must_be_shared(self):
        rows = evidence()
        for row in rows[:2]:
            row.update(causal_session=True, explicit_causal_session=True, session_id={'id': 'same'})
        self.assertEqual(wfr.classify(*rows)[:2], ('no_violation', False))
        rows[1]['session_id'] = {'id': 'different'}
        self.assertEqual(wfr.classify(*rows)[:2], ('inconclusive', None))

    def test_unavailability_separate(self):
        self.assertEqual(wfr.availability({'status': 'failure', 'error_type': 'NotPrimaryError'}),
                         'unavailable')
        self.assertEqual(wfr.availability(None), 'not_run')

    def test_failed_read_never_uses_later_write_as_proof(self):
        for status in ('timeout', 'failure'):
            for missing in (False, True):
                with self.subTest(status=status, missing=missing):
                    rows = evidence(missing)
                    rows[0]['status'] = status
                    self.assertEqual(wfr.classify(*rows)[:2], ('inconclusive', None))

    def test_failed_write_matrix_is_availability_not_consistency(self):
        for status, error_type, outcome in (
                ('timeout', 'NetworkTimeout', 'timeout'),
                ('failure', 'NotPrimaryError', 'unavailable'),
                ('failure', 'OperationFailure', 'error')):
            for missing in (False, True):
                with self.subTest(status=status, error_type=error_type, missing=missing):
                    rows = evidence(missing)
                    rows[1].update(status=status, error_type=error_type)
                    self.assertEqual(wfr.classify(*rows)[:2], ('inconclusive', None))
                    self.assertEqual(wfr.availability(rows[1]), outcome)

    def test_missing_or_wrong_dependency_marker_is_inconclusive(self):
        for marker in (None, {'id': 't:dependent_write', 'depends_on_version': 0}):
            with self.subTest(marker=marker):
                rows = evidence(True)
                rows[2]['returned_document']['y'] = marker
                self.assertEqual(wfr.classify(*rows)[:2], ('inconclusive', None))

    def test_final_history_cannot_override_atomic_preimage(self):
        # Later recovery (or a stale audit) is not the dependent write's execution history.
        for missing in (False, True):
            with self.subTest(missing=missing):
                rows = evidence(missing)
                rows[2]['returned_document']['x_history'] = [0, 1] if missing else [0]
                expected = ('candidate_violation', None) if missing else ('no_violation', False)
                self.assertEqual(wfr.classify(*rows)[:2], expected)


class WfrRunnerTests(unittest.TestCase):
    def run_mock_trial(self, scenario='normal', config='C1', recovery_failure=False,
                       controller_failure=False, write_timeout=False, read_status='success',
                       read_version=1, write_error=None, protocol=wfr.PROTOCOL):
        """Exercise sequencing and finally without starting containers or causing faults."""
        args = SimpleNamespace(experiment_id='synthetic', scenario=scenario,
                               database='test', timeout_ms=15000, uri='mongodb://unused/', wfr_protocol=protocol)
        client, controller, events, ops = (MagicMock() for _ in range(4))
        operations = []
        sequence = []
        trial_id = f'synthetic-wfr-{config}-{scenario}-0001'
        document = {'_id': trial_id, 'x_version': 1, 'x_history': [0, 1]}

        def execute(active, writer, meta, name, action, **kwargs):
            operations.append((active, name, kwargs))
            sequence.append(name)
            row = {**meta, 'status': 'success', 'operation_id': trial_id + ':' + name,
                   'target_node': 'mongo1:27017', 'latency_ms': 1,
                   'returned_document': deepcopy(document),
                   'explicit_causal_session': config == 'C4',
                   'session_id': {'id': 'test-session'} if config == 'C4' else None}
            if name == 'primary_audit':
                row['returned_document']['y'] = {'id': trial_id + ':dependent_write',
                                                 'depends_on_version': 1}
            if scenario == 'replication-partition' and name == 'dependent_write':
                row.update(target_node='mongo2:27017', returned_document={
                    '_id': trial_id, 'x_version': 1 if protocol == wfr.PROTOCOL_V2 else 0,
                    'x_history': [0, 1] if protocol == wfr.PROTOCOL_V2 else [0]})
            if write_timeout and name == 'dependent_write':
                row.update(status='timeout', returned_document=None)
            if name == 'predecessor_read':
                row['status'] = read_status
                row['returned_document'] = ({'_id': trial_id, 'x_version': read_version,
                                             'x_history': [0, 1] if read_version == 1 else [0]}
                                            if read_status == 'success' else None)
            if write_error and name == 'dependent_write':
                row.update(status='failure', error_type=write_error, returned_document=None)
            return row

        def request(action, *a, **kw):
            sequence.append(action)
            if controller_failure and action in ('crash', 'partition'):
                raise RuntimeError('synthetic lost controller response')
            if recovery_failure and action == 'recover':
                raise RuntimeError('synthetic failed recovery')
            return {}

        controller.request.side_effect = request
        monitor = MagicMock()
        monitor.finish.return_value = {'node': 'mongo2'}

        def inspect(node, *a):
            return {'node': node, 'hello': {'secondary': node != 'mongo1'}, 'document': {'_id': trial_id,
                    'x_version': 1 if node == 'mongo1' else 0,
                    'x_history': [0, 1] if node == 'mongo1' else [0]}}

        with TemporaryDirectory() as temp, \
                patch.object(wfr, 'make_client', return_value=MagicMock()), \
                patch.object(wfr, 'wait_healthy', return_value={'hello': {'primary': 'mongo1:27017'}}), \
                patch.object(wfr, 'wait_convergence', return_value={}), \
                patch.object(wfr, 'node_evidence', side_effect=inspect), \
                patch.object(wfr, 'ElectionObserver', return_value=monitor), \
                patch.object(wfr, 'execute_operation', side_effect=execute):
            result = wfr.run_trial(client, ops, events, controller, args, get_config(config),
                                   1, Path(temp))
        return result, operations, sequence

    def test_normal_c4_read_write_share_client_and_session(self):
        row, operations, _ = self.run_mock_trial(config='C4')
        self.assertEqual(row['classification'], 'no_violation')
        read = next(o for o in operations if o[1] == 'predecessor_read')
        write = next(o for o in operations if o[1] == 'dependent_write')
        self.assertIs(read[0], write[0])
        self.assertIs(read[2]['session'], write[2]['session'])
        self.assertIsNotNone(read[2]['session'])
        seed = next(o for o in operations if o[1] == 'seed_x')
        self.assertIsNot(seed[0], read[0])

    def test_v2_partition_injected_after_read_not_before_seed(self):
        row, operations, sequence = self.run_mock_trial(scenario='replication-partition',
                                                       config='C4', protocol=wfr.PROTOCOL_V2)
        self.assertEqual(row['classification'], 'no_violation')
        self.assertLess(sequence.index('seed_x'), sequence.index('predecessor_read'))
        self.assertLess(sequence.index('predecessor_read'), sequence.index('partition'))
        self.assertLess(sequence.index('partition'), sequence.index('dependent_write'))
        self.assertEqual(row['protocol_version'], wfr.PROTOCOL_V2)
        read = next(o for o in operations if o[1] == 'predecessor_read')
        write = next(o for o in operations if o[1] == 'dependent_write')
        self.assertIs(read[2]['session'], write[2]['session'])

    def test_s2_targets_secondary_and_recovers_after_write(self):
        row, _, sequence = self.run_mock_trial(scenario='secondary-stop', protocol=wfr.PROTOCOL_V2)
        self.assertEqual(row['fault_node'], 'mongo2')
        self.assertLess(sequence.index('predecessor_read'), sequence.index('crash'))
        self.assertLess(sequence.index('crash'), sequence.index('dependent_write'))
        self.assertIn('recover', sequence)
        self.assertTrue(row['recovery_completed'])

    def test_v2_failed_read_never_injects_partition(self):
        row, _, sequence = self.run_mock_trial(scenario='replication-partition',
                                               protocol=wfr.PROTOCOL_V2, read_status='timeout')
        self.assertNotIn('partition', sequence)
        self.assertNotIn('dependent_write', sequence)
        self.assertEqual(row['classification'], 'inconclusive')

    def test_crash_between_read_and_write_and_recovery(self):
        row, _, sequence = self.run_mock_trial(scenario='primary-crash')
        self.assertEqual(row['classification'], 'no_violation')
        self.assertLess(sequence.index('predecessor_read'), sequence.index('crash'))
        self.assertLess(sequence.index('crash'), sequence.index('dependent_write'))
        self.assertLess(sequence.index('dependent_write'), sequence.index('recover'))
        self.assertTrue(row['recovery_completed'])

    def test_partition_keeps_candidate_and_rollback_unassessed(self):
        row, _, sequence = self.run_mock_trial(scenario='replication-partition')
        self.assertEqual(row['classification'], 'candidate_violation')
        self.assertIsNone(row['is_violation'])
        self.assertEqual(row['rollback'], 'not_assessed')
        self.assertLess(sequence.index('partition'), sequence.index('seed_x'))
        self.assertIn('collect', sequence)

    def test_lost_fault_response_still_recovers(self):
        row, _, sequence = self.run_mock_trial(scenario='primary-crash', controller_failure=True)
        self.assertIn('recover', sequence)
        self.assertTrue(row['recovery_completed'])
        self.assertEqual(row['classification'], 'inconclusive')
        self.assertNotIn('dependent_write', sequence)

    def test_recovery_failure_preserves_original_observation(self):
        row, _, _ = self.run_mock_trial(scenario='primary-crash', recovery_failure=True)
        self.assertFalse(row['recovery_completed'])
        self.assertIn('synthetic failed recovery', row['recovery_error'])
        self.assertEqual(row['classification'], 'no_violation')

    def test_write_timeout_not_rescued_by_successful_audit(self):
        row, _, _ = self.run_mock_trial(write_timeout=True)
        self.assertEqual(row['classification'], 'inconclusive')
        self.assertFalse(row['write_acknowledged'])

    def test_candidate_excluded_from_violation_denominator(self):
        row, _, _ = self.run_mock_trial(scenario='replication-partition')
        summary = wfr.summarize([row])[0]
        self.assertEqual(summary['evaluable'], 0)
        self.assertIsNone(summary['violation_rate'])
        self.assertEqual(summary['workload_completion_rate'], 1)

    def test_failed_predecessor_skips_write_and_crash(self):
        for status in ('timeout', 'failure'):
            with self.subTest(status=status):
                row, _, sequence = self.run_mock_trial(scenario='primary-crash', read_status=status)
                self.assertEqual(row['classification'], 'inconclusive')
                self.assertIsNone(row['is_violation'])
                self.assertEqual(row['write_outcome'], 'not_run')
                self.assertIsNone(row['depends_on_version'])
                self.assertNotIn('dependent_write', sequence)
                self.assertNotIn('crash', sequence)

    def test_wrong_predecessor_version_is_invalid_trial(self):
        row, _, sequence = self.run_mock_trial(read_version=0)
        self.assertEqual(row['classification'], 'invalid_trial')
        self.assertIsNone(row['is_violation'])
        self.assertFalse(row['write_acknowledged'])
        self.assertNotIn('dependent_write', sequence)

    def test_failed_partition_read_still_recovers(self):
        row, _, sequence = self.run_mock_trial(scenario='replication-partition', read_status='timeout')
        self.assertIn('partition', sequence)
        self.assertIn('recover', sequence)
        self.assertNotIn('dependent_write', sequence)
        self.assertTrue(row['recovery_completed'])
        self.assertEqual(row['classification'], 'inconclusive')

    def test_failed_write_preserves_availability_and_recovery(self):
        row, _, sequence = self.run_mock_trial(scenario='primary-crash', write_error='NotPrimaryError')
        self.assertEqual(row['classification'], 'inconclusive')
        self.assertEqual(row['write_outcome'], 'unavailable')
        self.assertFalse(row['write_acknowledged'])
        self.assertIsNone(row['is_violation'])
        self.assertIn('recover', sequence)

    def test_summary_keeps_four_classes_and_failure_counts_separate(self):
        cases = ({}, {'scenario': 'replication-partition'}, {'read_version': 0},
                 {'write_timeout': True}, {'write_error': 'NotPrimaryError'})
        trials = [self.run_mock_trial(**kwargs)[0] for kwargs in cases]
        summary = wfr.summarize(trials)[0]
        self.assertEqual(summary['trials'], 5)
        for name, count in (('no_violation', 1), ('candidate_violation', 1), ('invalid_trial', 1),
                            ('inconclusive', 2), ('confirmed', 0), ('evaluable', 1),
                            ('write_timeout', 1), ('write_unavailable', 1), ('write_not_run', 1)):
            self.assertEqual(summary[name], count, name)
        self.assertEqual(summary['violation_rate'], 0)
        self.assertEqual(summary['workload_completion_rate'], 2 / 4)

    def test_no_evaluable_trials_report_null_rate(self):
        for kwargs in ({'read_version': 0}, {'write_timeout': True},
                       {'write_error': 'NotPrimaryError'}):
            with self.subTest(kwargs=kwargs):
                row, _, _ = self.run_mock_trial(**kwargs)
                summary = wfr.summarize([row])[0]
                self.assertEqual(summary['evaluable'], 0)
                self.assertIsNone(summary['violation_rate'])


if __name__ == '__main__':
    unittest.main()
