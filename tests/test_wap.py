"""Publication must be tied to the exact audited candidate and original main head."""

from unittest.mock import Mock

import pytest

from gridtime.quality.scan import QualityError
from gridtime.transform.wap import publish


def refs():
    nessie = Mock()
    nessie.ref.return_value = {"name": "silver-wap", "hash": "candidate"}
    nessie.merge.return_value = {"wasApplied": True}
    return nessie


def test_audit_failure_never_merges():
    nessie = refs()
    audit = Mock(side_effect=QualityError("duplicates"))
    with pytest.raises(QualityError):
        publish(nessie, "silver-wap", "base", audit=audit)
    nessie.merge.assert_not_called()


def test_audit_connection_error_never_merges():
    nessie = refs()
    with pytest.raises(ConnectionError):
        publish(nessie, "silver-wap", "base", audit=Mock(side_effect=ConnectionError()))
    nessie.merge.assert_not_called()


def test_failed_result_never_merges():
    nessie = refs()
    with pytest.raises(RuntimeError, match="Audit did not pass"):
        publish(nessie, "silver-wap", "base", audit=Mock(return_value={"passed": False}))
    nessie.merge.assert_not_called()


def test_changed_candidate_never_merges():
    nessie = refs()
    nessie.ref.side_effect = [{"hash": "one"}, {"hash": "two"}]
    with pytest.raises(RuntimeError, match="changed during audit"):
        publish(nessie, "silver-wap", "base", audit=Mock(return_value={"passed": True}))
    nessie.merge.assert_not_called()


def test_only_audited_commit_merges_with_original_main_hash():
    nessie = refs()
    audit = Mock(return_value={"passed": True, "report_path": "report.json"})
    result = publish(nessie, "silver-wap", "original-main", audit=audit)
    audit.assert_called_once_with("silver", "lakehouse_wap")
    nessie.merge.assert_called_once_with(
        {"name": "silver-wap", "hash": "candidate"}, "original-main"
    )
    assert result["audited_hash"] == "candidate"


def test_merge_conflict_propagates_without_retrying():
    nessie = refs()
    nessie.merge.side_effect = RuntimeError("conflict")
    with pytest.raises(RuntimeError, match="conflict"):
        publish(nessie, "silver-wap", "base", audit=Mock(return_value={"passed": True}))
    assert nessie.merge.call_count == 1


def test_no_new_data_is_a_successful_audited_noop():
    nessie = refs()
    report = publish(
        nessie,
        "silver-wap",
        "candidate",
        audit=Mock(return_value={"passed": True, "report_path": "report.json"}),
    )
    assert report["audited_hash"] == "candidate"
    nessie.merge.assert_not_called()


def test_main_change_during_noop_audit_is_rejected():
    nessie = refs()
    nessie.ref.side_effect = [{"hash": "base"}, {"hash": "base"}, {"hash": "new"}]
    with pytest.raises(RuntimeError, match="Main changed"):
        publish(
            nessie,
            "silver-wap",
            "base",
            audit=Mock(return_value={"passed": True, "report_path": "report.json"}),
        )
    nessie.merge.assert_not_called()
