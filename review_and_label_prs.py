# review_and_label_prs.py
import json, re, subprocess, sys
from pathlib import Path

REPO = "AdityaShett/dag-failure-agent"
LOG_PATH = Path("label_decisions.json")

def gh_json(args):
    result = subprocess.run(["gh"] + args, capture_output=True, text=True, check=True)
    return json.loads(result.stdout)

def extract_sections(body: str):
    rc_match = re.search(r"## Automated Root Cause Analysis\n\n(.*?)\n\n## Proposed Fix", body, re.DOTALL)
    diff_match = re.search(r"```diff\n(.*?)\n```", body, re.DOTALL)
    root_cause = rc_match.group(1).strip() if rc_match else "(not found)"
    diff = diff_match.group(1).strip() if diff_match else "(not found)"
    return root_cause, diff

def is_fallback(diff: str) -> bool:
    return "# Agent RCA Test" in diff

def load_decisions():
    return json.loads(LOG_PATH.read_text()) if LOG_PATH.exists() else {}

def save_decision(decisions, pr_number, verdict, title):
    decisions[str(pr_number)] = {"title": title, "verdict": verdict}
    LOG_PATH.write_text(json.dumps(decisions, indent=2))

def main():
    prs = gh_json(["pr", "list", "--repo", REPO, "--state", "open",
                   "--json", "number,title,body", "--limit", "100"])
    prs = [pr for pr in prs if re.search(r"for dag[1-6]\.", pr["title"])]
    decisions = load_decisions()

    print(f"{len(prs)} open PRs. {len(decisions)} already labeled this run.\n")

    for pr in prs:
        num, title, body = pr["number"], pr["title"], pr["body"]
        if str(num) in decisions:
            continue

        root_cause, diff = extract_sections(body)
        fallback = is_fallback(diff)

        print("=" * 70)
        print(f"#{num}: {title}")
        if fallback:
            print("[NOTE] This is a fallback filler diff, not a real fix — recommend closing.")
        print("\n--- Root cause ---")
        print(root_cause[:400])
        print("\n--- Diff ---")
        print(diff[:600])
        print()

        while True:
            choice = input("[m]erge / [c]lose / [s]kip / [q]uit: ").strip().lower()
            if choice in ("m", "c", "s", "q"):
                break

        if choice == "q":
            print("Stopping. Decisions saved so far are in label_decisions.json.")
            break
        if choice == "s":
            continue

        if choice == "m":
            subprocess.run(["gh", "pr", "ready", str(num), "--repo", REPO])
            subprocess.run(["gh", "pr", "merge", str(num), "--repo", REPO, "--squash", "--admin"])
            save_decision(decisions, num, "merged", title)
        elif choice == "c":
            subprocess.run(["gh", "pr", "close", str(num), "--repo", REPO])
            save_decision(decisions, num, "closed", title)

    print(f"\nDone. {len(decisions)} PRs labeled so far, saved to {LOG_PATH}.")

if __name__ == "__main__":
    main()