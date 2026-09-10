import re
from unidiff import PatchSet


def sanitize_diff_text(diff_text: str) -> str:
    """
    Cleans up common LLM-diff-generation artifacts before parsing.

    Confirmed in this project's processor logs: Gemini's raw diff output
    routinely arrives wrapped in ```diff / ``` fences, and blank context
    lines inside hunks sometimes drop their required leading space. Both
    cause PatchSet(...) to raise or misparse hunk boundaries.
    """
    if not diff_text:
        return diff_text

    text = diff_text.strip()

    # Strip ALL leading/trailing fence lines, not just one layer. Confirmed
    # in real PRs (#317-319): Gemini sometimes emits doubled/nested fences
    # (```diff\n```diff\n<diff>\n```\n```), and a single regex substitution
    # left one fence layer behind, which is exactly what caused those PRs'
    # UnidiffParseError: Hunk is shorter than expected.
    while True:
        new_text = re.sub(r"^```(?:diff|patch)?\s*\n", "", text)
        if new_text == text:
            break
        text = new_text

    while True:
        new_text = re.sub(r"\n?```\s*$", "", text)
        if new_text == text:
            break
        text = new_text

    # Repair blank context lines: inside a hunk, a completely empty line
    # must retain its required leading space, which markdown renderers and
    # LLMs routinely strip.
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


def _locate_hunk(lines, expected, hint_start):
    n = len(expected)
    if n == 0:
        return hint_start
    expected_norm = [e.rstrip("\n") for e in expected]

    candidate = lines[hint_start:hint_start + n]
    if [l.rstrip("\n") for l in candidate] == expected_norm:
        return hint_start

    for start in range(len(lines) - n + 1):
        window = [l.rstrip("\n") for l in lines[start:start + n]]
        if window == expected_norm:
            return start

    return None


def apply_unified_diff(original_content: str, diff_text: str) -> str:
    if diff_text.strip() == "NO_CONFIDENT_FIX":
        return original_content

    diff_text = sanitize_diff_text(diff_text)

    path = PatchSet(diff_text)
    lines = original_content.splitlines(keepends=True)

    for patched_file in path:
        for hunk in patched_file:
            hint_start = hunk.source_start - 1
            expected = [line.value for line in hunk if not line.is_added]

            start = _locate_hunk(lines, expected, hint_start)
            if start is None:
                raise ValueError(
                    f"Diff context not found anywhere in file (hunk claimed "
                    f"line {hunk.source_start}): file content does not match "
                    f"expected patch context."
                )

            new_lines = [line.value for line in hunk if not line.is_removed]
            lines[start:start + hunk.source_length] = new_lines

    return "".join(lines)