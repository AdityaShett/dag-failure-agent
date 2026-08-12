import os

from github import Github, Auth
from agent.diff_utils import apply_unified_diff


def open_draft_pr(state: dict) -> dict:
    print("ENTERED OPEN_PR")
    try:
        auth = Auth.Token(os.environ["GITHUB_TOKEN"])
        gh = Github(auth=auth)

        repo = gh.get_repo("adityashetty-CGEMU/dag-failure-agent")

        base_branch = repo.default_branch
        base_sha = repo.get_branch(base_branch).commit.sha

        branch_name = (
            f"agent-fix/"
            f"{state['dag_id']}-"
            f"{state['task_id']}-"
            f"{state['run_id'][:8]}"
        )

        print(f"Using repo: {repo.full_name}")
        print(f"Base branch: {base_branch}")
        print(f"Creating branch: {branch_name}")

        repo.create_git_ref(
            ref=f"refs/heads/{branch_name}",
            sha=base_sha,
        )

        # Temporary test target because the repository
        # currently does not contain dags/dag1.py
        file_path = "dag1.py"

        print(f"Updating file: {file_path}")

        contents = repo.get_contents(
            file_path,
            ref=branch_name,
        )

        try:
            patched_source = apply_unified_diff(
                contents.decoded_content.decode("utf-8"),
                state["proposed_fix"],
            )
        except Exception:
            patched_source = (
                contents.decoded_content.decode("utf-8")
                + "\n\n"
                + "## Agent RCA Test\n"
                + f"DAG: {state['dag_id']}\n"
                + f"Task: {state['task_id']}\n"
            )

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

        pr_body = (
            f"## Automated root-cause analysis\n\n"
            f"{state.get('root_cause', 'No RCA available')}\n\n"
            f"## Proposed fix\n"
            f"```diff\n"
            f"{state.get('proposed_fix', 'NO_FIX')}\n"
            f"```\n\n"
            f"**This PR was opened automatically. "
            f"A human must review before merging.**\n\n"
            f"Run: `{state['run_id']}`\n"
            f"Task: `{state['task_id']}`"
        )

        pr = repo.create_pull(
            title=(
                f"[agent] Fix for "
                f"{state['dag_id']}."
                f"{state['task_id']} failure"
            ),
            body=pr_body,
            head=branch_name,
            base=base_branch,
        )

        print(f"PR created: {pr.html_url}")

        return {
            "pr_url": pr.html_url
        }

    except Exception as e:
        print(f"GITHUB ERROR: {e}")
        raise