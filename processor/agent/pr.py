import os
import uuid
import json
from github import Auth, Github
from google.cloud import storage as gcs_storage
from unidiff.errors import UnidiffParseError
from agent.diff_utils import apply_unified_diff

_outcomes_client = gcs_storage.Client()
_OUTCOMES_BUCKET = os.environ.get("OUTCOMES_BUCKET")


def _record_pending_outcome(pr_number, dag_id, task_id, run_id, root_cause, proposed_fix, confidence_record_id=None):
    if not _OUTCOMES_BUCKET:
        return
    bucket = _outcomes_client.bucket(_OUTCOMES_BUCKET)
    blob = bucket.blob(f"pending/{pr_number}.json")
    blob.upload_from_string(json.dumps({
        "pr_number": pr_number, "dag_id": dag_id, "task_id": task_id,
        "run_id": run_id, "root_cause": root_cause, "proposed_fix": proposed_fix,
        "confidence_record_id": confidence_record_id,
    }))


def open_draft_pr(state: dict) -> dict:
    print("ENTERED OPEN_PR")
    print(f"Repo from state: {state['github_repo']}")
    print(f"Target file from state: {state['target_file']}")

    try:
        if not state.get("github_repo"):
            raise ValueError("github_repo missing from state")
        if not state.get("target_file"):
            raise ValueError("target_file missing from state")

        auth = Auth.Token(os.environ["GITHUB_TOKEN"])
        gh = Github(auth=auth)
        repo = gh.get_repo(state["github_repo"])
        file_path = state["target_file"]

        base_branch = repo.default_branch
        base_sha = repo.get_branch(base_branch).commit.sha
        branch_name = f"agent-fix/{state['dag_id']}-{uuid.uuid4().hex[:8]}"

        repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=base_sha)

        contents = repo.get_contents(file_path, ref=branch_name)
        current_source = contents.decoded_content.decode("utf-8")

        # Split exception handling: distinguish "diff didn't parse at all"
        # from "diff parsed but context didn't match the file" from anything
        # unexpected, and record WHY in diff_applied/fallback_reason instead
        # of silently merging filler text under an honest-looking title.
        diff_applied = True
        fallback_reason = None
        try:
            patched_source = apply_unified_diff(current_source, state["proposed_fix"])
        except UnidiffParseError as e:
            diff_applied = False
            fallback_reason = f"UnidiffParseError: {e}"
        except ValueError as e:
            diff_applied = False
            fallback_reason = f"ValueError: {e}"
        except Exception as e:
            diff_applied = False
            fallback_reason = f"{type(e).__name__}: {e}"

        if not diff_applied:
            print(f"Could not apply diff ({fallback_reason}). Falling back to test modification.")
            patched_source = (
                current_source + "\n\n" + "# Agent RCA Test\n"
                + f"# DAG: {state['dag_id']}\n" + f"# Task: {state['task_id']}\n"
            )

        repo.update_file(
            path=file_path,
            message=f"Agent-proposed fix for {state['task_id']} failure ({state['run_id']})",
            content=patched_source, sha=contents.sha, branch=branch_name,
        )

        # Fallback PRs are now ALWAYS labeled honestly, regardless of
        # confidence tier -- this is what directly prevents another
        # dag6.py-style incident (a fallback-filler PR merged under a
        # normal-looking title).
        if not diff_applied:
            title_prefix = "[agent][fallback-no-fix]"
        elif state.get("confidence_tier") == "medium":
            title_prefix = "[agent][medium-confidence]"
        else:
            title_prefix = "[agent]"

        pr_body = (
            f"## Automated Root Cause Analysis\n\n{state.get('root_cause', 'No RCA available')}\n\n"
            f"## Proposed Fix\n```diff\n{state.get('proposed_fix', 'NO_FIX')}\n```\n\n"
            f"**Confidence score:** {state.get('confidence_score')} ({state.get('confidence_tier')})\n\n"
            f"**Diff applied:** {diff_applied}"
            + (f" ({fallback_reason})" if fallback_reason else "")
            + "\n\n"
            f"**This Pull Request was opened automatically. A human review is required before merging.**\n\n"
            f"Run: `{state['run_id']}`\nTask: `{state['task_id']}`"
        )

        pr = repo.create_pull(
            title=f"{title_prefix} Fix for {state['dag_id']}.{state['task_id']} failure",
            body=pr_body, head=branch_name, base=base_branch, draft=True,
        )

        print(f"PR created: {pr.html_url}")

        _record_pending_outcome(
            pr.number, state["dag_id"], state["task_id"], state["run_id"],
            state.get("root_cause", ""), state.get("proposed_fix", ""),
            state.get("confidence_record_id"),
        )

        # Previously returned nothing -- processor_app.py's result.get("pr_url")
        # was always None (§6.2). Now returns both pr_url and diff_applied so
        # downstream code/state can act on whether a real fix was applied.
        return {"pr_url": pr.html_url, "diff_applied": diff_applied}

    except Exception as e:
        print(f"GITHUB ERROR: {e}")
        raise
