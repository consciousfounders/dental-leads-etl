#!/usr/bin/env python3
"""
Session Logger for Claude Code conversations.

Maintains a running session log and archives it on commit.
Designed to be called from git hooks or manually.

Usage:
    # Start/append to current session
    python scripts/log_session.py log "Built Apollo enrichment CLI"

    # Archive current session (called by post-commit hook)
    python scripts/log_session.py archive --commit abc1234

    # Generate ADR from session (if warranted)
    python scripts/log_session.py adr --title "Budget Tracking" --commit abc1234

    # Show current session
    python scripts/log_session.py show
"""

import argparse
import json
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path


SESSIONS_DIR = Path(__file__).parent.parent / "docs" / "sessions"
DECISIONS_DIR = Path(__file__).parent.parent / "docs" / "decisions"
CURRENT_SESSION = SESSIONS_DIR / ".current.json"


def ensure_dirs():
    """Ensure session and decision directories exist."""
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    DECISIONS_DIR.mkdir(parents=True, exist_ok=True)


def load_current_session() -> dict:
    """Load current session data."""
    if CURRENT_SESSION.exists():
        with open(CURRENT_SESSION, 'r') as f:
            return json.load(f)
    return {
        "started_at": datetime.now().isoformat(),
        "entries": [],
        "tags": set(),
        "files_discussed": set(),
        "decisions": [],
    }


def save_current_session(session: dict):
    """Save current session data."""
    ensure_dirs()
    # Convert sets to lists for JSON
    session_copy = session.copy()
    session_copy["tags"] = list(session.get("tags", []))
    session_copy["files_discussed"] = list(session.get("files_discussed", []))
    with open(CURRENT_SESSION, 'w') as f:
        json.dump(session_copy, f, indent=2)


def log_entry(message: str, entry_type: str = "note", tags: list = None, files: list = None):
    """Add an entry to the current session."""
    session = load_current_session()

    # Convert lists back to sets
    session["tags"] = set(session.get("tags", []))
    session["files_discussed"] = set(session.get("files_discussed", []))

    entry = {
        "timestamp": datetime.now().isoformat(),
        "type": entry_type,  # note, decision, question, code, command
        "message": message,
    }

    session["entries"].append(entry)

    if tags:
        session["tags"].update(tags)
    if files:
        session["files_discussed"].update(files)

    save_current_session(session)
    print(f"[{entry_type}] {message[:80]}...")


def extract_tags_from_commit(commit_hash: str) -> list:
    """Extract tags from commit message and changed files."""
    tags = set()

    try:
        # Get commit message
        msg = subprocess.check_output(
            ["git", "log", "-1", "--format=%B", commit_hash],
            text=True
        ).strip()

        # Extract conventional commit type
        match = re.match(r'^(\w+)(?:\(([^)]+)\))?:', msg)
        if match:
            tags.add(match.group(1))  # feat, fix, docs, etc.
            if match.group(2):
                tags.add(match.group(2))  # scope

        # Get changed files
        files = subprocess.check_output(
            ["git", "diff-tree", "--no-commit-id", "--name-only", "-r", commit_hash],
            text=True
        ).strip().split('\n')

        for f in files:
            if f.startswith('enrichment/'):
                tags.add('enrichment')
            elif f.startswith('warehouse/'):
                tags.add('warehouse')
            elif f.startswith('ops/'):
                tags.add('ops')
            elif f.startswith('docs/decisions/'):
                tags.add('adr')

            # File type tags
            if f.endswith('.sql'):
                tags.add('sql')
            elif f.endswith('.py'):
                tags.add('python')

    except subprocess.CalledProcessError:
        pass

    return list(tags)


def archive_session(commit_hash: str = None):
    """Archive current session to a dated markdown file."""
    if not CURRENT_SESSION.exists():
        print("No current session to archive")
        return

    session = load_current_session()

    if not session.get("entries"):
        print("Current session is empty")
        return

    # Get commit info if provided
    commit_info = ""
    commit_tags = []
    if commit_hash:
        commit_tags = extract_tags_from_commit(commit_hash)
        try:
            commit_msg = subprocess.check_output(
                ["git", "log", "-1", "--format=%s", commit_hash],
                text=True
            ).strip()
            commit_info = f"\n**Commit:** `{commit_hash[:7]}` - {commit_msg}\n"
        except subprocess.CalledProcessError:
            commit_info = f"\n**Commit:** `{commit_hash[:7]}`\n"

    # Merge tags
    all_tags = set(session.get("tags", []))
    all_tags.update(commit_tags)

    # Generate filename
    date_str = datetime.now().strftime("%Y-%m-%d")

    # Find unique filename
    base_name = f"{date_str}_session"
    counter = 1
    while True:
        if counter == 1:
            filename = f"{base_name}.md"
        else:
            filename = f"{base_name}_{counter}.md"

        if not (SESSIONS_DIR / filename).exists():
            break
        counter += 1

    # Generate markdown
    md_content = f"""# Session Log - {date_str}

**Started:** {session.get('started_at', 'unknown')}
**Archived:** {datetime.now().isoformat()}
**Tags:** {', '.join(sorted(all_tags)) if all_tags else 'none'}
{commit_info}
## Files Discussed

{chr(10).join('- ' + f for f in sorted(session.get('files_discussed', []))) or '- None tracked'}

## Session Log

"""

    for entry in session.get("entries", []):
        ts = entry.get("timestamp", "")[:16].replace("T", " ")
        entry_type = entry.get("type", "note")
        msg = entry.get("message", "")

        if entry_type == "decision":
            md_content += f"\n### [{ts}] DECISION\n{msg}\n"
        elif entry_type == "command":
            md_content += f"\n**[{ts}]** `{msg}`\n"
        elif entry_type == "code":
            md_content += f"\n**[{ts}]** Code:\n```\n{msg}\n```\n"
        else:
            md_content += f"\n**[{ts}]** {msg}\n"

    # Write archive
    archive_path = SESSIONS_DIR / filename
    with open(archive_path, 'w') as f:
        f.write(md_content)

    # Clear current session
    CURRENT_SESSION.unlink()

    print(f"Session archived to: {archive_path}")
    return archive_path


def show_session():
    """Display current session."""
    if not CURRENT_SESSION.exists():
        print("No current session")
        return

    session = load_current_session()

    print(f"\n{'='*50}")
    print(f"Current Session - Started {session.get('started_at', 'unknown')[:16]}")
    print(f"{'='*50}")
    print(f"Tags: {', '.join(session.get('tags', [])) or 'none'}")
    print(f"Files: {len(session.get('files_discussed', []))} tracked")
    print(f"Entries: {len(session.get('entries', []))}")
    print()

    for entry in session.get("entries", [])[-10:]:  # Last 10
        ts = entry.get("timestamp", "")[-8:-3]  # HH:MM
        entry_type = entry.get("type", "note")[:4]
        msg = entry.get("message", "")[:60]
        print(f"  [{ts}] {entry_type}: {msg}")


def get_next_adr_number() -> int:
    """Get next ADR number."""
    existing = list(DECISIONS_DIR.glob("[0-9][0-9][0-9]_*.md"))
    if not existing:
        return 1
    numbers = [int(p.stem[:3]) for p in existing if p.stem[:3].isdigit()]
    return max(numbers) + 1 if numbers else 1


def generate_adr(title: str, commit_hash: str = None):
    """Generate a new ADR from current session context."""
    ensure_dirs()

    num = get_next_adr_number()
    slug = re.sub(r'[^a-z0-9]+', '_', title.lower()).strip('_')
    filename = f"{num:03d}_{slug}.md"

    commits_ref = f"`{commit_hash[:7]}`" if commit_hash else "(pending)"

    content = f"""# ADR-{num:03d}: {title}

**Date:** {datetime.now().strftime('%Y-%m-%d')}
**Status:** Accepted
**Commits:** {commits_ref}

## Context

[What prompted this decision?]

## Decision

[What did we decide?]

## Alternatives Considered

1. **Option A** - Description
   - Pros: ...
   - Cons: ...

## Consequences

- Positive: ...
- Negative: ...

## Session Reference

Claude Code session: {datetime.now().strftime('%Y-%m-%d')}
"""

    adr_path = DECISIONS_DIR / filename
    with open(adr_path, 'w') as f:
        f.write(content)

    print(f"Created ADR: {adr_path}")
    return adr_path


def main():
    parser = argparse.ArgumentParser(description="Session logging for Claude Code")
    subparsers = parser.add_subparsers(dest='command')

    # Log command
    log_parser = subparsers.add_parser('log', help='Add entry to session')
    log_parser.add_argument('message', help='Log message')
    log_parser.add_argument('--type', '-t', default='note',
                          choices=['note', 'decision', 'question', 'code', 'command'])
    log_parser.add_argument('--tags', '-g', nargs='+', help='Tags')
    log_parser.add_argument('--files', '-f', nargs='+', help='Files discussed')

    # Archive command
    archive_parser = subparsers.add_parser('archive', help='Archive current session')
    archive_parser.add_argument('--commit', '-c', help='Associated commit hash')

    # Show command
    subparsers.add_parser('show', help='Show current session')

    # ADR command
    adr_parser = subparsers.add_parser('adr', help='Generate ADR')
    adr_parser.add_argument('--title', '-t', required=True, help='ADR title')
    adr_parser.add_argument('--commit', '-c', help='Associated commit hash')

    args = parser.parse_args()

    if args.command == 'log':
        log_entry(args.message, args.type, args.tags, args.files)
    elif args.command == 'archive':
        archive_session(args.commit)
    elif args.command == 'show':
        show_session()
    elif args.command == 'adr':
        generate_adr(args.title, args.commit)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
