from unidiff import PatchSet

def apply_unified_diff(original_content: str, diff_text: str) -> str:

    if diff_text.strip() == "NO_CONFIDENT_FIX":
        return original_content

    path = PatchSet(diff_text)
    lines = original_content.splitlines(keepends=True)

    for patched_file in path:
        for hunk in patched_file:
            start = hunk.source_start - 1

            expected = [
                line.value for line in hunk
                if not line.is_added
            ]
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