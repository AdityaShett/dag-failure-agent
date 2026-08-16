from unidiff import PatchSet

def apply_unified_diff(original_content : str, diff_text : str) -> str:

    if diff_text.strip() == "NO_CONFIDENT_FIX":
        return original_content
    
    path =  PatchSet(diff_text)
    lines = original_content.splitlines(keepends=True)

    for patched_file in path:
        for hunk in patched_file:
            start = hunk.source_start - 1
            new_lines = [line.value for line in hunk if not line.is_removed]
            lines[start:start + hunk.source_length] = new_lines

    return "".join(lines)