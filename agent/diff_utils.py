import re
from unidiff import PatchSet
from unidiff.errors import UnidiffParseError


def sanitize_diff_text(diff_text: str) -> str:
    text = diff_text.strip()

    # Strip a wrapping ```diff / ``` fence
    fence_match = re.match(r"^```(?:diff|patch)?\s*\n(.*?)\n?```$", text, re.DOTALL)
    if fence_match:
        text = fence_match.group(1)
    else:
        # Strip stray unmatched fence lines
        lines = text.splitlines()
        if lines and lines[0].strip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines)

    # Repair blank context lines inside a hunk (must keep leading space)
    lines = text.split("\n")
    repaired = []
    in_hunk = False
    for line in lines:
        if line.startswith("@@"):
            in_hunk = True
            repaired.append(line)
        elif line.startswith("---") or line.startswith("+++") or line.startswith("diff "):
            in_hunk = False
            repaired.append(line)
        elif in_hunk and line == "":
            repaired.append(" ")
        else:
            repaired.append(line)

    return "\n".join(repaired)


def apply_unified_diff(original_content: str, diff_text: str) -> str:
    if diff_text.strip() == "NO_CONFIDENT_FIX":
        return original_content

    diff_text = sanitize_diff_text(diff_text)
    patch = PatchSet(diff_text)
    lines = original_content.splitlines(keepends=True)

    for patched_file in patch:
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