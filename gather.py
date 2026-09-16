from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "gathered_files.txt"

# Directories to completely exclude
SKIP_DIRS = {
    ".venv",
    ".venv312",
    "venv",
    "__pycache__",
    ".git",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "node_modules",
    "dist",
    "build",
}

# Files to exclude
SKIP_FILES = {
    "label_decisions.json",
    "gather.py",
    "gathered_files.txt",
}

# Binary/generated file extensions to exclude
SKIP_EXTENSIONS = {
    ".pyc",
    ".pyo",
    ".exe",
    ".dll",
    ".so",
    ".dylib",
    ".bin",
    ".db",
    ".sqlite",
    ".sqlite3",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".ico",
    ".pdf",
    ".zip",
    ".tar",
    ".gz",
    ".7z",
    ".rar",
    ".mp3",
    ".mp4",
    ".wav",
    ".avi",
    ".mov",
    ".woff",
    ".woff2",
    ".ttf",
    ".otf",
}


def should_skip(path: Path) -> bool:
    """Return True if the path should be excluded."""

    # Exclude anything inside a skipped directory
    if any(part in SKIP_DIRS for part in path.parts):
        return True

    # Exclude specific files
    if path.name in SKIP_FILES:
        return True

    # Exclude binary/generated file types
    if path.suffix.lower() in SKIP_EXTENSIONS:
        return True

    return False


def gather_files():
    file_count = 0

    with OUTPUT.open("w", encoding="utf-8") as out:
        for path in sorted(ROOT.rglob("*")):
            if not path.is_file():
                continue

            if should_skip(path):
                continue

            relative_path = path.relative_to(ROOT)

            try:
                content = path.read_text(encoding="utf-8")
            except (UnicodeDecodeError, PermissionError, OSError):
                continue

            out.write("\n")
            out.write("=" * 80)
            out.write(f"\nFILE: {relative_path}\n")
            out.write("=" * 80)
            out.write("\n\n")
            out.write(content)
            out.write("\n")

            file_count += 1

    print(f"Gathered {file_count} files.")
    print(f"Output: {OUTPUT}")


if __name__ == "__main__":
    gather_files()