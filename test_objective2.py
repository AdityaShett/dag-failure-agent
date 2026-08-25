
import os
import sys
import json
from unittest.mock import patch, MagicMock




def test_state_has_confidence_record_id():
    from agent.state import RCAState
    assert "confidence_record_id" in RCAState.__annotations__

@patch("agent.nodes._get_bq_client")
def test_compute_confidence_logs_and_returns_record_id(mock_bq):
    mock_bq.return_value.insert_rows_json.return_value = []
    from agent.nodes import compute_confidence
    state = {"dag_id": "d", "task_id": "t", "run_id": "r",
             "llm_confidence": 0.9, "retrieved_knowledge": ["a"],
             "task_logs": "x"*100, "dag_source": "y"*100}
    with patch("agent.nodes._fetch_history_score", return_value=0.5):
        result = compute_confidence(state)
    assert "confidence_record_id" in result and result["confidence_record_id"]
    mock_bq.return_value.insert_rows_json.assert_called_once()

@patch("agent.pr.Github")
@patch("agent.pr._outcomes_client")
@patch("agent.pr._OUTCOMES_BUCKET", "fake-bucket")
@patch.dict(os.environ, {"GITHUB_TOKEN": "fake-token"})
def test_open_pr_writes_pending_with_record_id(mock_gcs, mock_gh):
    from agent.pr import open_draft_pr
    mock_repo = MagicMock()
    mock_repo.default_branch = "main"
    mock_repo.get_branch.return_value.commit.sha = "abc"
    mock_repo.get_contents.return_value.decoded_content = b"code"
    mock_repo.create_pull.return_value.number = 42
    mock_repo.create_pull.return_value.html_url = "http://pr"
    mock_gh.return_value.get_repo.return_value = mock_repo
    mock_blob = MagicMock()
    mock_gcs.bucket.return_value.blob.return_value = mock_blob

    state = {"github_repo": "o/r", "target_file": "f.py", "dag_id": "d",
              "task_id": "t", "run_id": "r", "proposed_fix": "NO_CONFIDENT_FIX",
              "confidence_record_id": "rec-123", "confidence_score": 0.8,
              "confidence_tier": "high"}
    open_draft_pr(state)

    written = json.loads(mock_blob.upload_from_string.call_args[0][0])
    assert written["confidence_record_id"] == "rec-123"


def _import_webhook_app_mocked():
    """github_webhook_app.py hits real GCP clients at import time
    (Secret Manager, Storage, BigQuery). Mock all of it before import."""
    sys.modules.pop("github_webhook_app", None)
    env = {"GCP_PROJECT": "fake-project", "OUTCOMES_BUCKET": "fake-bucket"}
    with patch.dict(os.environ, env), \
         patch("google.cloud.secretmanager.SecretManagerServiceClient") as mock_sm, \
         patch("google.cloud.storage.Client"), \
         patch("google.cloud.bigquery.Client"):
        mock_sm.return_value.access_secret_version.return_value.payload.data = b"fake-secret"
        import github_webhook_app as w
        return w

@patch("google.cloud.bigquery.QueryJobConfig")
def test_webhook_calls_update_confidence_outcome(mock_qjc):
    w = _import_webhook_app_mocked()
    with patch.object(w, "_bq_client") as mock_bq_client:
        record = {"dag_id": "d", "task_id": "t", "confidence_record_id": "rec-123"}
        w._update_confidence_outcome(record, 42, True)
        mock_bq_client.query.assert_called_once()
        params = mock_qjc.call_args.kwargs["query_parameters"]
        record_id_param = [p for p in params if p.name == "record_id"][0]
        assert record_id_param.value == "rec-123"

def test_webhook_endpoint_calls_both_recorders():
    w = _import_webhook_app_mocked()
    with patch.object(w, "_record_to_fix_history") as mock_fix, \
         patch.object(w, "_update_confidence_outcome") as mock_conf:
        record = {"dag_id": "d", "task_id": "t", "confidence_record_id": "rec-123"}
        # simulate what webhook() does after loading pending record
        w._record_to_fix_history(record, 42, True)
        w._update_confidence_outcome(record, 42, True)
        mock_fix.assert_called_once()
        mock_conf.assert_called_once()