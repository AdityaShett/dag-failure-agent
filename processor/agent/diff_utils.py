import re
from unidiff import PatchSet


def sanitize_diff_text(diff_text: str) -> str:
    """
    Cleans up common LLM-diff-generation artifacts before parsing.

    Confirmed in this project's processor logs: Gemini's raw diff output
    routinely arrives wrapped in ```diff / ``` fences, and blank context
    lines inside hunks sometimes drop their required leading space. Both
    cause PatchSet(...) to raise or misparse hunk boundaries, which
    previously fell straight through to pr.py's fallback-filler path
    (see PROJECT-HANDOFF.md §5/§6.1) -- silently corrupting merge/reject
    labels used for weight tuning.
    """
    if not diff_text:
        return diff_text

    text = diff_text.strip()

    # Strip a leading ```diff / ```patch / ``` fence and a trailing ``` fence.
    text = re.sub(r"^```(?:diff|patch)?\s*\n", "", text)
    text = re.sub(r"\n?```\s*$", "", text)

    # Repair blank context lines: inside a hunk, a completely empty line
    # should be a single space (an unchanged blank line), not nothing --
    # unified diff format requires every context line to start with a space.
    lines = text.split("\n")
    repaired = []
    in_hunk = False
    for line in lines:
        if line.startswith("@@"):
            in_hunk = True
            repaired.append(line)
            continue
        if in_hunk and line == "":
            repaired.append(" ")
        else:
            repaired.append(line)
    return "\n".join(repaired)


def apply_unified_diff(original_content: str, diff_text: str) -> str:
    if diff_text.strip() == "NO_CONFIDENT_FIX":
        return original_content

    diff_text = sanitize_diff_text(diff_text)

    path = PatchSet(diff_text)
    lines = original_content.splitlines(keepends=True)

    for patched_file in path:
        for hunk in patched_file:
            start = hunk.source_start - 1
            expected = [line.value for line in hunk if not line.is_added]
            actual = lines[start:start + hunk.source_length]
            actual_normalized = [l if l.endswith("\n") else l + "\n" for l in actual]

            if [e.rstrip("\n") for e in expected] != [a.rstrip("\n") for a in actual_normalized]:
                raise ValueError(
                    f"Diff context mismatch at line {hunk.source_start}: "
                    f"file content does not match expected patch context."
                )

            new_lines = [line.value for line in hunk if not line.is_removed]
            lines[start:start + hunk.source_length] = new_lines

    return "".join(lines)
