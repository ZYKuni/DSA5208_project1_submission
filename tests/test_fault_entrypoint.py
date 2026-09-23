"""Host dispatch regression tests; no Docker commands or real faults."""
import contextlib
import importlib.util
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from experiments.run_wfr_faults import parse_args as parse_wfr

SPEC = importlib.util.spec_from_file_location(
    'fault_entrypoint', Path(__file__).resolve().parents[1] / 'scripts/run-fault-experiment.py')
host = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(host)


class FaultEntrypointTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = patch.object(host, 'ROOT', Path(self.temp.name))
        root.start()
        self.addCleanup(root.stop)

    def parse(self, *args):
        return host.parse_args(['--experiment-id', 'synthetic', *args])

    def rejects(self, *args):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            self.parse(*args)
        self.assertEqual(error.exception.code, 2)

    def test_default_mw_unchanged(self):
        args = self.parse()
        self.assertEqual((args.workload, args.scenario, args.configs, args.trials,
                          args.timeout_ms, args.retry_writes),
                         ('mw', 'primary-crash', ['C1', 'C4'], 5, 30000, True))
        self.assertIn('experiments.run_faults', host.runner_command(args, 'test-runner'))

    def test_ryw_defaults_unchanged(self):
        args = self.parse('--workload', 'ryw')
        self.assertEqual((args.timeout_ms, args.retry_writes, args.trials), (30000, True, 5))
        self.assertIn('experiments.run_ryw_faults', host.runner_command(args, 'test-runner'))

    def test_mr_dispatches_both_supported_faults(self):
        for scenario in ('secondary-stop', 'primary-crash'):
            args = self.parse('--workload', 'mr', '--scenario', scenario)
            command = host.runner_command(args, 'test-runner')
            self.assertIn('experiments.run_mr_faults', command)
            self.assertIn('--sample-stage', command)

    def test_mr_rejects_faults_outside_its_protocol(self):
        for scenario in ('replication-partition', 'two-secondary-stop'):
            self.rejects('--workload', 'mr', '--scenario', scenario, '--no-retry-writes')

    def test_formal_mr_requires_one_hundred_trials(self):
        self.rejects('--workload', 'mr', '--scenario', 'primary-crash',
                     '--sample-stage', 'formal', '--trials', '99')
        args = self.parse('--workload', 'mr', '--scenario', 'primary-crash',
                          '--sample-stage', 'formal', '--trials', '100')
        self.assertEqual(args.sample_stage, 'formal')

    def test_legacy_partition_still_requires_explicit_no_retry(self):
        for workload in ('mw', 'ryw'):
            with self.subTest(workload=workload):
                self.rejects('--workload', workload, '--scenario', 'replication-partition')
                args = self.parse('--workload', workload, '--scenario', 'replication-partition',
                                  '--no-retry-writes')
                self.assertFalse(args.retry_writes)

    def test_wfr_dispatch_arguments_are_accepted_by_runner(self):
        for scenario in ('normal', 'primary-crash', 'replication-partition'):
            with self.subTest(scenario=scenario), patch.dict(os.environ, {'FAULT_CONTROL': '/synthetic'}):
                args = self.parse('--workload', 'wfr', '--scenario', scenario, '--trials', '1')
                command = host.runner_command(args, 'test-runner')
                index = command.index('-m')
                self.assertEqual(command[index + 1], 'experiments.run_wfr_faults')
                parsed = parse_wfr(command[index + 2:])
                self.assertEqual((parsed.scenario, parsed.timeout_ms, parsed.retry_writes),
                                 (scenario, 15000, False))
                for name in ('FAULT_CONTROL', 'SOURCE_BASE_COMMIT', 'SOURCE_WORKTREE_STATUS'):
                    self.assertIn(name, command)

    def test_wfr_rejects_explicit_retry_before_docker(self):
        with patch.object(host, 'command') as command, patch.object(host, 'recover_all') as recover:
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                host.main(['--experiment-id', 'synthetic', '--workload', 'wfr', '--retry-writes'])
            command.assert_not_called()
            recover.assert_not_called()

    def test_wfr_v2_stage_and_plan_forwarding(self):
        with patch.dict(os.environ, {'FAULT_CONTROL': '/synthetic'}):
            args = self.parse('--workload', 'wfr', '--scenario', 'secondary-stop',
                              '--wfr-protocol', 'wfr-committed-dependency-v2',
                              '--sample-stage', 'formal', '--trials', '100', '--plan-id', 'approved-test')
            command = host.runner_command(args, 'test-runner')
            self.assertIn('experiments.run_wfr_faults', command)
            parsed = parse_wfr(command[command.index('-m') + 2:])
            self.assertEqual((parsed.sample_stage, parsed.plan_id), ('formal', 'approved-test'))

    def test_wfr_formal_requires_explicit_protocol_and_plan(self):
        self.rejects('--workload', 'wfr', '--sample-stage', 'formal', '--trials', '100')
        self.rejects('--workload', 'wfr', '--scenario', 'secondary-stop')
        self.rejects('--workload', 'wfr', '--scenario', 'two-secondary-stop',
                     '--wfr-protocol', 'wfr-committed-dependency-v2')

    def test_explicit_budgets_and_configs_forwarded(self):
        args = self.parse('--workload', 'wfr', '--timeout-ms', '9000', '--configs', 'C2', 'C3',
                          '--trials', '2', '--no-retry-writes')
        command = host.runner_command(args, 'test-runner')
        self.assertEqual(command[command.index('--timeout-ms') + 1], '9000')
        self.assertEqual(command[command.index('--trials') + 1], '2')
        self.assertEqual(command[command.index('--configs') + 1:], ['C2', 'C3'])

    def test_normal_is_wfr_only(self):
        for workload in ('mw', 'ryw', 'mr'):
            self.rejects('--workload', workload, '--scenario', 'normal')

    def test_bad_inputs_rejected(self):
        for args in (('--trials', '0'), ('--timeout-ms', '0'), ('--configs', 'C1', 'C1'),
                     ('--experiment-id', '../escape'), ('--workload', 'unknown')):
            with self.subTest(args=args):
                self.rejects(*args)

    def test_existing_output_rejected(self):
        (host.ROOT / 'results/raw/synthetic').mkdir(parents=True)
        self.rejects('--workload', 'wfr')

    def test_host_launches_wfr_and_recovers_on_nonzero_exit(self):
        process = MagicMock()
        process.poll.return_value = 7
        process.returncode = 7
        with patch.object(host, 'command', return_value=MagicMock(stdout='synthetic\n')), \
                patch.object(host.subprocess, 'Popen', return_value=process) as launch, \
                patch.object(host, 'recover_all') as recover, \
                contextlib.redirect_stdout(io.StringIO()):
            code = host.main(['--experiment-id', 'synthetic', '--workload', 'wfr'])
        self.assertEqual(code, 7)
        self.assertIn('experiments.run_wfr_faults', launch.call_args.args[0])
        self.assertEqual(launch.call_args.kwargs['env']['FAULT_CONTROL'],
                         '/workspace/results/fault-control/synthetic')
        recover.assert_called_once_with()
        self.assertTrue((host.ROOT / 'results/fault-control/synthetic/host-recovery.json').exists())


if __name__ == '__main__':
    unittest.main()
