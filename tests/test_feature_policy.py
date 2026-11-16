"""Unit tests for the prespecified financial feature and event-label policies."""
from datetime import date, timedelta
import math

import pytest

from fdic_pipeline.feature_policy import (
    FEATURE_NAMES, FEATURE_SPECS, FEATURE_RATIONALES, STUDY_END, LAST_LABEL_QUARTER,
    DEVELOPMENT_END, TEST_START, TEST_END, EventLedger,
    EXCLUDED_PREDICTOR_FIELDS, quarter_index, add_quarters, split_name,
    finite, ratio, growth, calculate_features, label_at_quarter,
    unfit_preprocessor,
)
from fdic_pipeline.feature_pipeline import event_ledgers, policy_lock


def ledger(*, last=date(2015, 12, 31), failure=None, assistance=(), end=None, uncertain=None):
    return EventLedger(last_report=last, failure_day=failure,
                       assistance_days=assistance, institution_end=end,
                       uncertain_reason=uncertain)


def test_feature_contract_no_identifiers_or_future_event_variables():
    assert len(FEATURE_NAMES) == 21
    assert len(set(FEATURE_NAMES)) == 21
    assert tuple(row[0] for row in FEATURE_SPECS) == FEATURE_NAMES
    assert tuple(FEATURE_RATIONALES) == FEATURE_NAMES
    assert not (set(FEATURE_NAMES) & EXCLUDED_PREDICTOR_FIELDS)
    assert 'RESTYPE' not in FEATURE_NAMES and 'FAILDATE' not in FEATURE_NAMES


@pytest.mark.parametrize('day,split', [
    (date(1984,3,31),'development'), (date(2006,12,31),'development'),
    (date(2007,3,31),'purge_gap'),(date(2007,12,31),'purge_gap'),
    (date(2008,3,31),'test_holdout'),(date(2014,12,31),'test_holdout'),
    (date(2015,3,31),'unused_post_test'),(date(2025,6,30),'unused_post_test'),
    (date(2025,9,30),'label_immature'),
])
def test_prespecified_split_boundaries(day,split):
    assert split_name(day) == split


def test_split_has_four_full_quarters_between_dev_and_test():
    assert DEVELOPMENT_END == date(2006,12,31)
    assert TEST_START == date(2008,3,31)
    assert TEST_END == date(2014,12,31)
    assert add_quarters(DEVELOPMENT_END,4)==date(2007,12,31)
    assert quarter_index(TEST_START)-quarter_index(DEVELOPMENT_END)==5
    assert LAST_LABEL_QUARTER == date(2025,6,30)
    assert add_quarters(LAST_LABEL_QUARTER,4)==STUDY_END


def test_quarter_arithmetic_handles_leap_dates():
    assert add_quarters(date(2007,3,31),4)==date(2008,3,31)
    assert add_quarters(date(2019,12,31),1)==date(2020,3,31)
    with pytest.raises(ValueError):
        quarter_index(date(2012,4,1))


@pytest.mark.parametrize('value',[None, True, float('inf'),-float('inf'),float('nan'),'bad'])
def test_invalid_numeric_values_remain_missing(value):
    assert finite(value) is None


def test_risk_ratios_preserve_economically_extreme_values():
    assert ratio(-10,100)==-10
    assert ratio(120,100)==120
    assert ratio(10,0) is None
    assert growth(125,100)==25
    assert growth(50,0) is None


def source(day=date(2008,6,30),**extras):
    row = {'REPDTE':day,'ASSET':125.,'EQ':10.,'DEP':110.,'BRO':8.,
           'CHBAL':12.,'SC':20.,'LNLSGR':85.,'NCLNLS':3.,
           'ROA':1.2,'ROE':10.,'NIMY':2.9,'RBC1AAJ':9.5,
           'RBCRWAJ':13.,'PRIOR_REPORT_COUNT':4,'REPORT_GAP_QUARTERS':0,
           'DQ_EXTREME_ROE':False,'MISSING_FINANCIAL_COUNT':2}
    row.update(extras)
    return row


def test_features_use_only_exact_calendar_lags():
    day=date(2008,6,30)
    q=quarter_index(day)
    previous={q-1:{'ASSET':100.,'DEP':100.,'ROA':2.0},
              q-4:{'ASSET':90.,'DEP':80.,'ROA':0.5}}
    features=calculate_features(source(day),previous)
    assert list(features)==list(FEATURE_NAMES)
    assert features['ASSET_GROWTH_QOQ_PCT']==25.0
    assert features['DEPOSIT_GROWTH_QOQ_PCT']==pytest.approx(10.0)
    assert features['ROA_CHANGE_YOY_PPT']==pytest.approx(.7)
    assert features['ASSET_GROWTH_YOY_PCT']==pytest.approx(100*(125/90-1))
    assert calculate_features(source(day),{q-2:previous[q-1]})['ASSET_GROWTH_QOQ_PCT'] is None


def test_future_failure_or_resolution_metadata_do_not_change_features():
    a=calculate_features(source(),{})
    b=calculate_features(source(FAILDATE='2008-08-01', RESTYPE='FAILURE',
                                  COST=999999, ACTIVE='0',TERMINAL_CLASS='confirmed_failure'),{})
    assert a==b


def test_failure_horizon_boundaries_and_availability():
    q=date(2008,3,31)
    assert label_at_quarter(q,ledger(failure=date(2009,3,31)))==(1,'failure_within_4q')
    assert label_at_quarter(q,ledger(failure=date(2009,4,1)))==(0,'observed_no_failure_4q')
    assert label_at_quarter(q,ledger(failure=q))[0] is None
    assert label_at_quarter(q,ledger(failure=q+timedelta(days=89)))[1]=='event_before_assumed_availability'
    assert label_at_quarter(q,ledger(failure=q+timedelta(days=91)))[0]==1
    assert label_at_quarter(date(2025,9,30),ledger())[1]=='label_immature'


def test_assistance_not_equated_with_failure_even_when_dated():
    q=date(2008,3,31)
    assert label_at_quarter(q,ledger(assistance=(date(2008,9,30),))) == (None,'competing_assistance')
    assert label_at_quarter(q,ledger(failure=date(2009,3,31),assistance=(date(2008,9,30),)))[0] is None
    assert label_at_quarter(q,ledger(assistance=(date(2007,9,30),)))[0]==0


def test_merger_and_unresolved_followup_never_become_positive_or_default_negative():
    q=date(2008,3,31)
    assert label_at_quarter(q,ledger(end=date(2008,12,31)))[1]=='competing_other_exit'
    assert label_at_quarter(q,ledger(last=date(2008,6,30)))[1]=='follow_up_incomplete'
    assert label_at_quarter(q,ledger(last=date(2009,3,31)))[0]==0
    assert label_at_quarter(q,ledger(uncertain='multiple_failure_dates'))[1]=='uncertain_event_evidence'


def test_source_ledger_restype_separates_507_failures_and_assistance_without_hardcoding():
    terminal=[{'CERT':'1','LAST_REPDTE':'2008-03-31','INSTITUTION_END_DATE':'2008-10-01'},
              {'CERT':'2','LAST_REPDTE':'2009-03-31','INSTITUTION_END_DATE':''},
              {'CERT':'3','LAST_REPDTE':'2009-03-31','INSTITUTION_END_DATE':''}]
    fail=[{'CERT':1,'FAILDATE':'2008-10-01','RESTYPE':'FAILURE'},
          {'CERT':2,'FAILDATE':'2008-10-01','RESTYPE':'ASSISTANCE'},
          {'CERT':3,'FAILDATE':'2009-04-01','RESTYPE':'UNKNOWN'},
          {'CERT':None,'FAILDATE':'2008-10-01','RESTYPE':'FAILURE'}]
    ledger_map,audit=event_ledgers(fail,terminal)
    assert ledger_map[1].failure_day == date(2008,10,1)
    assert ledger_map[1].institution_end is None
    assert ledger_map[2].failure_day is None
    assert ledger_map[2].uncertain_reason is None
    assert ledger_map[3].uncertain_reason == 'unclassifiable_failure_reference'
    assert audit['test_2008_2014_failure_source_rows']==2
    assert audit['test_2008_2014_distinct_failure_certs_in_panel']==1
    assert audit['source_rows_missing_cert']==1


def test_multiple_failure_dates_are_censored_not_duplicated():
    terms=[{'CERT':'8','LAST_REPDTE':'2006-12-31','INSTITUTION_END_DATE':''}]
    events=[{'CERT':8,'RESTYPE':'FAILURE','FAILDATE':'2007-08-01'},
            {'CERT':8,'RESTYPE':'FAILURE','FAILDATE':'2007-09-01'}]
    index,audit=event_ledgers(events,terms)
    assert index[8].uncertain_reason=='multiple_failure_dates'
    assert label_at_quarter(date(2006,12,31), index[8])[0] is None


def test_source_event_after_last_report_does_not_invoke_unknown_future_feature():
    terms=[{'CERT':'9','LAST_REPDTE':'2010-03-31','INSTITUTION_END_DATE':'2011-05-01'}]
    events=[{'CERT':9,'RESTYPE':'FAILURE','FAILDATE':'2011-05-01'}]
    index,_=event_ledgers(events,terms)
    assert index[9].institution_end is None
    assert label_at_quarter(date(2010,3,31),index[9])[0]==0


def test_policy_lock_has_fixed_90_day_assumption_and_fit_boundary():
    obj=policy_lock()
    assert obj['assumed_availability_lag_days']==90
    assert obj['assumption_is_verified_release_date'] is False
    assert obj['development_only_fit'] and obj['refit_preprocessing_in_every_part5_fold']
    assert obj['label_definition'].startswith('1{RESTYPE=FAILURE')


def test_unfitted_preprocessor_requires_training_fit():
    import numpy as np
    from sklearn.exceptions import NotFittedError
    p=unfit_preprocessor()
    with pytest.raises(NotFittedError):
        p.transform(np.array([[1.,2.],[3.,4.]]))
    data=np.array([[1.,4.],[2.,np.nan],[9.,7.]])
    p.fit(data)
    result=p.transform(data)
    assert np.isfinite(result).all()
    assert result.shape[0]==3
    assert p.named_steps['imputer'].statistics_[1]==pytest.approx(5.5)
