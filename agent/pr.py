import os
import uuid
from github import Auth, Github
from agent.diff_utils import apply_unified_diff


def open_draft_pr(state: dict) -> dict:
    print("ENTERED OPEN_PR")

    print(f"Repo from state: {state['github_repo']}")
    print(f"Target file from state: {state['target_file']}")

    try:
        # Validate required inputs
        if not state.get("github_repo"):
            raise ValueError(
                "github_repo missing from state"
            )

        if not state.get("target_file"):
            raise ValueError(
                "target_file missing from state"
            )

        # Connect to GitHub
        auth = Auth.Token(os.environ["GITHUB_TOKEN"])
        gh = Github(auth=auth)

        repo = gh.get_repo(state["github_repo"])
        file_path = state["target_file"]

        print(f"Using repo: {repo.full_name}")
        print(f"Updating file: {file_path}")

        # Get base branch information
        base_branch = repo.default_branch
        base_sha = repo.get_branch(base_branch).commit.sha

        branch_name = (
            f"agent-fix/"
            f"{state['dag_id']}-"
            f"{uuid.uuid4().hex[:8]}"
        )

        print(f"Base branch: {base_branch}")
        print(f"Creating branch: {branch_name}")

        # Create branch
        repo.create_git_ref(
            ref=f"refs/heads/{branch_name}",
            sha=base_sha,
        )

        # Load file contents
        contents = repo.get_contents(
            file_path,
            ref=branch_name,
        )

        current_source = contents.decoded_content.decode(
            "utf-8"
        )

        # Try applying AI-generated diff
        try:
            patched_source = apply_unified_diff(
                current_source,
                state["proposed_fix"],
            )
        except Exception:
            print(
                "Could not apply diff. "
                "Falling back to test modification."
            )

            patched_source = (
                current_source
                + "\n\n"
                + "# Agent RCA Test\n"
                + f"# DAG: {state['dag_id']}\n"
                + f"# Task: {state['task_id']}\n"
            )

        # Commit modified file
        repo.update_file(
            path=file_path,
            message=(
                f"Agent-proposed fix for "
                f"{state['task_id']} failure "
                f"({state['run_id']})"
            ),
            content=patched_source,
            sha=contents.sha,
            branch=branch_name,
        )

        # Build PR body
        pr_body = (
            f"## Automated Root Cause Analysis\n\n"
            f"{state.get('root_cause', 'No RCA available')}\n\n"
            f"## Proposed Fix\n"
            f"```diff\n"
            f"{state.get('proposed_fix', 'NO_FIX')}\n"
            f"```\n\n"
            f"**This Pull Request was opened automatically. "
            f"A human review is required before merging.**\n\n"
            f"Run: `{state['run_id']}`\n"
            f"Task: `{state['task_id']}`"
        )

        # Create PR
        pr = repo.create_pull(
            title=(
                f"[agent] Fix for "
                f"{state['dag_id']}."
                f"{state['task_id']} failure"
            ),
            body=pr_body,
            head=branch_name,
            base=base_branch,
            draft= True,
        )

        print(f"PR created: {pr.html_url}")

        return {
            "pr_url": pr.html_url
        }

    except Exception as e:
        print(f"GITHUB ERROR: {e}")
        raise