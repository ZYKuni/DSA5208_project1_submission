"""Analysis tests are independent of experiments/common.py and Docker."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

from analysis.unified import adapt, classification, load_sources, outcome, percentile, summarize
from analysis.plot_results import timeline_coordinates


def flat(violation=False, status='success'):
    return {'schema_version':'1.0.0', 'run_id':'r', 'trial_id':1, 'model':'RYW',
            'config':{'id':'C1'}, 'scenario':{'name':'normal'}, 'status':status,
            'violation':violation, 'operations':[
                {'stage':'predecessor_write','operation':'write','status':'success','latency_ms':2},
                {'stage':'successor_read','operation':'read','status':status,'latency_ms':100}]}


def summary_of(rows, fmt='schema-v1'):
    ts = adapt(rows, {}, fmt)
    for t in ts:
        t.update(source='synthetic', format=fmt, stage='pilot', selected=True, batch='test')
    return summarize(ts)


class UnifiedTests(unittest.TestCase):
    def test_true_false_denominator(self):
        a, b = flat(True), flat(False)
        b['trial_id'] = 2
        s = summary_of([a,b])[0]
        self.assertEqual((s['confirmed_violations'],s['evaluable'],s['violation_rate']), (1,2,.5))

    def test_timeout_is_not_pass_and_not_success_latency(self):
        s = summary_of([flat(None,'timeout')])[0]
        self.assertIsNone(s['violation_rate'])
        self.assertIsNone(s['read_p95_ms'])
        self.assertEqual(s['operation_success_rate'],.5)
        self.assertEqual(s['operation_statuses']['timeout'],1)

    def test_candidate_flag_does_not_override_adjudicated_label(self):
        self.assertEqual(classification({'record_type':'trial_result','classification':'violation',
                                         'is_violation':True,'candidate_violation':True}), 'confirmed_violation')

    def test_candidates_rollback_invalid_never_promoted(self):
        for label in ('candidate_violation','rollback_observed','invalid_trial','inconclusive'):
            with self.subTest(label=label):
                self.assertEqual(classification({'classification':label,'is_violation':True}),label)

    def test_legacy_latency_not_fabricated(self):
        row = {'model':'RYW','config_id':'C1','trial_id':1,'scenario':'normal',
               'status':'success','violation':False,'latency_ms':15}
        s = summary_of([row], 'legacy')[0]
        self.assertIsNone(s['read_p95_ms'])
        self.assertIsNone(s['write_p95_ms'])
        self.assertIsNone(s['operation_success_rate'])
        self.assertIsNone(s['unrecorded_slots'])

    def test_recovery_unknown_not_success(self):
        s = summary_of([flat()])[0]
        self.assertEqual(s['recovery_unknown'],1)
        self.assertIsNone(s['recovery_success_rate'])

    def test_mr_excludes_setup_and_post_heal(self):
        row = {'kind':'diagnostic_result','document_id':'r','base_config':'C4',
               'status':'timeout','violation':None,'scenario':'network_partition',
               'operations':[{'stage':stage,'operation':'read','status':status,'latency_ms':ms}
                for stage,status,ms in [('baseline_w3','success',1),('first_read','success',2),
                                        ('second_read','timeout',7000),('post_heal_read','success',3)]]}
        t = adapt([row], {'recovery_verified':True}, 'mr-diagnostic')[0]
        self.assertEqual(len(t['operations']),2)
        self.assertEqual(t['classification'],'inconclusive')
        self.assertTrue(t['recovery'])

    def test_no_terminal_is_incomplete(self):
        op = {'record_type':'operation','experiment':'wfr','trial_id':'t','config_id':'C1',
              'scenario':'normal','operation':'predecessor_read','phase':'workload','status':'success'}
        s = summary_of([op], 'operation-log')[0]
        self.assertEqual(s['classifications'],{'incomplete':1})
        self.assertIsNone(s['violation_rate'])
        self.assertEqual(s['unrecorded_slots'],1)

    def test_duplicate_terminal_rejected(self):
        t = {'record_type':'trial_result','trial_id':'t'}
        with self.assertRaises(ValueError):
            adapt([t,t],{},'operation-log')

    def test_write_concern_error_not_success(self):
        self.assertEqual(outcome({'status':'success','attempts':[{'write_concern_error':{'code':64}}]}), 'failure')

    def test_unavailable_and_timeout_separate(self):
        self.assertEqual(outcome({'status':'failure','error_type':'NotPrimaryError'}), 'unavailable')
        self.assertEqual(outcome({'status':'timeout','error_type':'NetworkTimeout'}), 'timeout')

    def test_percentile_and_missing(self):
        self.assertEqual(percentile([0,100]),95)
        self.assertIsNone(percentile([None,-1,float('nan')]))

    def test_stage_config_variant_batch_separate(self):
        base = adapt([flat()],{},'schema-v1')[0]
        base.update(source='s',format='schema-v1',stage='pilot',selected=True,batch='b')
        ts = [base]
        for key,value in [('stage','formal'),('config','C4'),('variant','direct'),('batch','other')]:
            row = copy.deepcopy(base); row[key] = value; ts.append(row)
        self.assertEqual(len(summarize(ts)),5)

    def test_identical_source_is_deduplicated_and_input_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); folder = root/'results/pilot/ryw/normal/handoff'; folder.mkdir(parents=True)
            text = json.dumps(flat())+'\n'
            for name in ('a.jsonl','b.jsonl'):
                (folder/name).write_text(text)
            catalog, ts = load_sources(root)
            self.assertEqual(len(ts),1)
            self.assertIsNotNone(catalog[1]['duplicate_of'])
            self.assertEqual((folder/'a.jsonl').read_text(),text)

    def test_explicit_reproduction_source_is_selected_without_changing_stage(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            folder = root/'results/pilot/ryw/normal/debug'
            folder.mkdir(parents=True)
            source = folder/'ryw-C1-normal-reproduce-001.jsonl'
            source.write_text(json.dumps(flat())+'\n')
            pattern = 'results/pilot/ryw/normal/debug/*-reproduce-001.jsonl'
            catalog, trials = load_sources(root, [pattern])
            self.assertTrue(catalog[0]['selected'])
            self.assertEqual(catalog[0]['selection_reason'], 'explicit-reproduction-source')
            self.assertEqual((trials[0]['stage'], trials[0]['selected']), ('pilot', True))

    def test_unmatched_explicit_source_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, 'matched no evidence'):
                load_sources(Path(tmp), ['results/pilot/missing-*.jsonl'])

    @unittest.skipUnless(
        (Path(__file__).resolve().parents[1]/'results/raw/c-wfr-normal-pilot-0914-01').exists(),
        'archived cross-model fixture bundle is not included in the compact submission',
    )
    def test_archived_regression(self):
        root = Path(__file__).resolve().parents[1]
        catalog, ts = load_sources(root)
        summaries = summarize(ts)
        wfr = [s for s in summaries if s['selected'] and s['model']=='WFR'
               and s['variant'] != 'wfr-committed-dependency-v2']
        self.assertEqual(sum(s['trials'] for s in wfr),10)
        self.assertEqual(sum(s['evaluable'] for s in wfr),8)
        mr = [s for s in summaries if s['selected'] and s['model']=='MR' and s['scenario']=='replication-partition']
        self.assertEqual(sum(s['trials'] for s in mr),10)
        self.assertEqual(sum(s['confirmed_violations'] for s in mr),5)

    def test_secondary_archives_match_source_summaries(self):
        root = Path(__file__).resolve().parents[1]
        _, ts = load_sources(root)
        groups = summarize(ts)
        for path in sorted((root/'results/archive').glob('a-s*/summary.json')):
            source = str((path.parent/'operations.jsonl').relative_to(root))
            for original in json.loads(path.read_text()):
                with self.subTest(run=path.parent.name, config=original['config_id']):
                    g = next(g for g in groups if source in g['sources'] and g['config']==original['config_id'])
                    self.assertTrue(g['selected'])
                    self.assertEqual(g['trials'],original['trials'])
                    self.assertEqual(g['evaluable'],original['evaluable'])
                    self.assertEqual(g['confirmed_violations'],original['violations'])
                    self.assertEqual(g['recovery_success'],original['recovered'])
                    self.assertEqual(g['violation_rate'],original['violation_rate'])
                    self.assertEqual(g['missing_operation_evidence'],0)

    def test_secondary_ryw_uses_write_one(self):
        op = {'record_type':'operation','trial_id':'t','experiment':'ryw','config_id':'C1',
              'scenario':'secondary-stop','phase':'workload','operation':'write_1','status':'success'}
        t = adapt([op],{'protocol':'secondary-stop-v1','s3_phase':'immediate'},'operation-log')[0]
        self.assertEqual(t['operations'][0]['kind'],'write')
        self.assertEqual(t['expected_operations'],2)

    def test_mr_fault_protocol_excludes_independent_writer_setup(self):
        base = {'record_type':'operation','trial_id':'t','experiment':'mr','config_id':'C4',
                'scenario':'primary-crash','status':'success'}
        rows = [
            {**base,'phase':'setup','operation':'independent_writer_update'},
            {**base,'phase':'workload','operation':'first_read'},
            {**base,'phase':'workload','operation':'second_read'},
            {**base,'record_type':'trial_result','classification':'no_violation',
             'is_violation':False,'recovery_completed':True},
        ]
        trial = adapt(rows, {'protocol':'mr-fault-between-reads-v1'}, 'operation-log')[0]
        self.assertEqual([op['source_name'] for op in trial['operations']],
                         ['first_read','second_read'])
        self.assertEqual(trial['variant'], 'mr-fault-between-reads-v1')

    def test_planned_denominator_preserves_skips(self):
        op = {'record_type':'operation','trial_id':'t','experiment':'mw','config_id':'C4',
              'scenario':'two-secondary-stop','phase':'workload','operation':'write_1','status':'timeout'}
        terminal = {**op,'record_type':'trial_result','classification':'inconclusive','is_violation':None,
                    'write_2_status':'not_run','read_status':'not_run'}
        t = adapt([op,terminal],{'protocol':'secondary-stop-v1','s3_phase':'immediate'},'operation-log')[0]
        t.update(source='s',format='operation-log',stage='formal',selected=True,batch='b')
        s = summarize([t])[0]
        self.assertEqual((s['operation_attempts'],s['operation_slots'],s['known_skipped']), (1,3,2))
        self.assertEqual(s['missing_operation_evidence'],0)
        self.assertEqual(s['planned_operation_success_rate'],0)

    def test_monotonic_axis_ignores_host_offsets_and_wall_duration(self):
        op = {'started_at':'2026-09-15T00:00:00Z','ended_at':'2026-09-15T01:00:00Z',
              'latency_ms':20,'monotonic_started_ns':2_000_000_000}
        t = {'operations':[op],'fault_at':'2099-01-01T00:00:00Z',
             'fault_monotonic_ns':1_000_000_000,'host_fault_at_unaligned':'1900-01-01T00:00:00Z'}
        _, spans, marks, basis = timeline_coordinates(t)
        self.assertEqual(spans,[(0,.02)])
        self.assertEqual(marks,{'fault receipt (runner)':-1})
        self.assertEqual(basis,'runner monotonic')

    def test_host_only_fault_not_plotted(self):
        t = {'operations':[{'started_at':'2026-09-15T00:00:00Z','latency_ms':1}],
             'host_fault_at_unaligned':'2099-01-01T00:00:00Z'}
        self.assertEqual(timeline_coordinates(t)[2],{})

    def test_unmapped_workload_fails_closed(self):
        op = {'record_type':'operation','trial_id':'t','experiment':'ryw','config_id':'C1',
              'scenario':'normal','phase':'workload','operation':'surprise','status':'success'}
        with self.assertRaises(ValueError):
            adapt([op],{},'operation-log')


if __name__ == '__main__':
    unittest.main()
