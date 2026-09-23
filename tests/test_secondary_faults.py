import unittest
from unittest.mock import patch
from types import SimpleNamespace
from datetime import datetime
from pymongo.errors import WTimeoutError
from experiments.common import NodeRecorder, record_operation, utc_now
from experiments.run_secondary_faults import select_targets, assess
from test_fault_entrypoint import host

class CompatibilityTests(unittest.TestCase):
    def test_schema_utc_and_failed_operation_preserved(self):
        operations=[]
        def fail():
            raise WTimeoutError('synthetic majority timeout')
        with self.assertRaises(WTimeoutError):
            record_operation(operations, NodeRecorder(), stage='write', operation='write',
                             document_id='synthetic', action=fail, requested_version=1)
        self.assertEqual(operations[0]['status'], 'timeout')
        for value in [utc_now(), operations[0]['started_at'], operations[0]['completed_at']]:
            self.assertTrue(value.endswith('Z'))
            self.assertIsNotNone(datetime.fromisoformat(value).tzinfo)

class SecondaryTests(unittest.TestCase):
    def test_targets_never_include_primary(self):
        snapshot={'status': {'members': [
            {'name': n+':27017', 'stateStr': 'PRIMARY' if n=='mongo2' else 'SECONDARY'}
            for n in ('mongo1', 'mongo2', 'mongo3')]}}
        self.assertEqual(select_targets(snapshot, 'secondary-stop'), ('mongo2',['mongo1']))
        self.assertEqual(select_targets(snapshot, 'two-secondary-stop'), ('mongo2',['mongo1','mongo3']))
        snapshot['status']['members'][0]['stateStr']='RECOVERING'
        with self.assertRaises(RuntimeError):
            select_targets(snapshot, 'secondary-stop')

    def test_unacknowledged_write_cannot_be_rescued_by_recovery(self):
        for workload in ('ryw', 'mw'):
            category, violation, _ = assess(workload, {'status':'timeout'}, None, None,
                {'status':'success', 'returned_document':{'version':1}}, None)
            self.assertEqual(category, 'inconclusive')
            self.assertIsNone(violation)

    def test_recovery_failure_invalidates_trial(self):
        self.assertEqual(assess('ryw', {'status':'success'},None,
            {'status':'success','returned_version':1},None,'recovery failed')[:2], ('invalid_trial',None))

    def test_dispatch_preserves_workload(self):
        for workload in ('mw','ryw'):
            for scenario in ('secondary-stop','two-secondary-stop','two-secondary-stop-settled'):
                args=host.parse_args(['--experiment-id','synthetic-secondary-test',
                                     '--workload',workload,'--scenario',scenario])
                command=host.runner_command(args,'test')
                self.assertIn('experiments.run_secondary_faults',command)
                self.assertEqual(command[command.index('--workload')+1],workload)

class RecoveryProtocolTests(unittest.TestCase):
    def test_lost_second_stop_response_recovers_both_nodes(self):
        from pathlib import Path
        from unittest.mock import MagicMock
        from experiments import run_secondary_faults as runner
        from experiments.configs import get_config
        snapshot={'hello':{'primary':'mongo2:27017'}, 'status':{'members':[
            {'name':n+':27017','stateStr':'PRIMARY' if n=='mongo2' else 'SECONDARY'}
            for n in ('mongo1','mongo2','mongo3')]}}
        controller=MagicMock()
        def request(action,node,**kwargs):
            if action=='crash' and node=='mongo3':
                raise TimeoutError('synthetic lost response after stop')
            return {'ok':True,'ended_at':utc_now()}
        controller.request.side_effect=request
        args=SimpleNamespace(experiment_id='synthetic',workload='mw',scenario='two-secondary-stop',
                             retry_writes=False,timeout_ms=5000,database='synthetic')
        with patch.object(runner,'wait_healthy',return_value=snapshot), \
             patch.object(runner,'wait_convergence',return_value={}), \
             patch.object(runner,'save'), \
             patch.object(runner,'execute_operation',side_effect=[{'status':'success'},
                         {'status':'success','returned_document':{'version':0}}]) as execute:
            result=runner.run_trial(MagicMock(),MagicMock(),MagicMock(),controller,args,
                                    get_config('C4'),1,Path('/synthetic'))
        self.assertEqual([(c.args[0],c.args[1]) for c in controller.request.call_args_list],
                         [('crash','mongo1'),('crash','mongo3'),('recover','mongo1'),('recover','mongo3')])
        self.assertEqual([c.args[3] for c in execute.call_args_list],['initialize','recovered_primary_audit'])
        self.assertEqual(result['classification'],'invalid_trial')
        self.assertIsNone(result['is_violation'])
        self.assertTrue(result['recovery_completed'])

    def test_formal_sampling_minimum_rejected_before_docker(self):
        import contextlib,io
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            host.parse_args(['--experiment-id','synthetic','--scenario','secondary-stop',
                             '--sample-stage','formal','--trials','20'])
        args=host.parse_args(['--experiment-id','synthetic','--scenario','secondary-stop',
                             '--sample-stage','formal','--trials','100'])
        self.assertIn('formal',host.runner_command(args,'test'))

class ClockEvidenceTests(unittest.TestCase):
    def setUp(self):
        import importlib.util
        from pathlib import Path
        spec=importlib.util.spec_from_file_location('secondary_validator',Path(__file__).resolve().parents[1]/'scripts/validate-secondary-run.py')
        self.module=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.trial={'trial_id':'test','stopped_nodes':['mongo1']}
        self.events=[{'event':'controller_request','trial_id':'test','request_id':'r1',
                      'action':'crash','node':'mongo1','time':'2026-09-15T00:00:00Z'},
                     {'event':'controller_response','request_id':'r1','action':'crash','node':'mongo1',
                      'ok':True,'state':{'Running':False},'time':'2026-09-15T00:00:01Z',
                      'ended_at':'2099-01-01T00:00:00Z'}]

    def test_host_clock_offset_does_not_invalidate_handshake(self):
        work=[{'started_at':'2026-09-15T00:00:02Z'}]
        self.assertEqual(self.module.verify_stop_order(self.events,self.trial,work),'legacy-runner-UTC')

    def test_actual_early_workload_still_rejected(self):
        with self.assertRaises(ValueError):
            self.module.verify_stop_order(self.events,self.trial,[{'started_at':'2026-09-15T00:00:00Z'}])

    def test_monotonic_order_survives_runner_wall_clock_adjustment(self):
        self.events[1]['monotonic_ns']=20
        work=[{'monotonic_started_ns':30,'started_at':'2020-01-01T00:00:00Z'}]
        self.assertEqual(self.module.verify_stop_order(self.events,self.trial,work),'runner-monotonic')
        work[0]['monotonic_started_ns']=10
        with self.assertRaises(ValueError):self.module.verify_stop_order(self.events,self.trial,work)

    def test_unmatched_receipt_is_not_evidence(self):
        self.events[1]['request_id']='unrelated'
        with self.assertRaises(ValueError):
            self.module.verify_stop_order(self.events,self.trial,[{'started_at':'2026-09-15T00:00:02Z'}])

    def test_running_target_is_not_confirmed_stop(self):
        self.events[1]['state']['Running']=True
        with self.assertRaises(ValueError):
            self.module.verify_stop_order(self.events,self.trial,[{'started_at':'2026-09-15T00:00:02Z'}])
