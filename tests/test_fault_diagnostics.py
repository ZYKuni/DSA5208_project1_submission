"""Negative controls: rollback evidence must not be inferred from RBID alone."""
import unittest
from experiments.fault_diagnostics import assess_fault, documents_converged
from experiments.test_monotonic_writes import history_entry


def fixture(missing=False):
    h1=history_entry('t:write_1',1)
    h2=history_entry('t:write_2',2)
    w1={'status':'success','operation_id':'t:write_1',
        'returned_document':{'version':0,'client_seq':0,'history':[]}}
    w2={'status':'success','operation_id':'t:write_2',
        'returned_document':{'version':0 if missing else 1,'client_seq':0 if missing else 1,
                             'history':[] if missing else [h1]}}
    final={'version':2,'client_seq':2,'history':[h2] if missing else [h1,h2]}
    audit={'status':'success','returned_document':final}
    before={'rbid':1,'document':{'history':[h1]}}
    after={'rbid':3,'document':final}
    proof={'rollback_completed_log':True,'matching_rollback_documents':[{'contains_w1':True}]}
    return w1,w2,audit,before,after,proof


class FaultEvidenceTests(unittest.TestCase):
    def test_ordered_writes_survive_crash(self):
        result=assess_fault(*fixture())
        self.assertEqual(result['classification'],'no_violation')
        self.assertEqual(result['rollback'],'not_observed_for_trial')

    def test_full_rollback_proof_is_separate_from_mw(self):
        result=assess_fault(*fixture(True))
        self.assertEqual(result['classification'],'rollback_observed')
        self.assertEqual(result['rollback'],'confirmed')
        self.assertFalse(result['candidate_violation'])
        self.assertIsNone(result['is_violation'])

    def test_rbid_change_alone_not_rollback(self):
        evidence=list(fixture(True))
        evidence[-1]={}
        self.assertEqual(assess_fault(*evidence)['rollback'],'not_confirmed')

    def test_unrelated_rollback_file_not_enough(self):
        evidence=list(fixture(True))
        evidence[-1]['matching_rollback_documents']=[{'contains_w1':False}]
        self.assertEqual(assess_fault(*evidence)['rollback'],'not_confirmed')

    def test_missing_completion_log_not_enough(self):
        evidence=list(fixture(True))
        evidence[-1]['rollback_completed_log']=False
        self.assertEqual(assess_fault(*evidence)['classification'],'candidate_violation')

    def test_unacknowledged_majority_write_not_confirmed_loss(self):
        evidence=list(fixture(True))
        evidence[0]['status']='timeout'
        evidence[1]=None
        result=assess_fault(*evidence)
        self.assertEqual(result['classification'],'inconclusive')
        self.assertNotEqual(result['rollback'],'confirmed')
        self.assertIsNone(result['write_1_survived'])


class RecoveryGateTests(unittest.TestCase):
    def test_old_history_blocks_recovery_even_if_roles_look_healthy(self):
        samples=[{'role':'PRIMARY','document':{'history':[2]}},
                 {'role':'SECONDARY','document':{'history':[1]}},
                 {'role':'SECONDARY','document':{'history':[2]}}]
        self.assertFalse(documents_converged(samples,{'history':[2]}))

    def test_rollback_transition_blocks_recovery(self):
        samples=[{'role':role,'document':{'version':2}}
                 for role in ['PRIMARY','SECONDARY','TRANSITION']]
        self.assertFalse(documents_converged(samples,{'version':2}))

    def test_all_direct_documents_and_roles_required(self):
        samples=[{'role':role,'document':{'version':2}}
                 for role in ['PRIMARY','SECONDARY','SECONDARY']]
        self.assertTrue(documents_converged(samples,{'version':2}))


if __name__=='__main__':
    unittest.main()
