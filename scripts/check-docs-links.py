#!/usr/bin/env python3
"""Fail if a relative link in the documentation points at a missing file or at
a heading that does not exist in the target file."""
import glob
import os
import re
import sys


def slug(heading):
    # GitHub's rule: lower-case, drop everything but letters, digits, spaces and hyphens.
    text = re.sub(r"`", "", heading.strip().lower())
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")


def anchors(path, cache={}):
    if path not in cache:
        found = set()
        in_block = False
        for line in open(path).read().split("\n"):
            if line.strip().startswith("```"):
                in_block = not in_block
            elif not in_block and re.match(r"#{1,6}\s", line):
                found.add(slug(re.sub(r"^#+\s*", "", line)))
        cache[path] = found
    return cache[path]


files = sorted(glob.glob("docs/*.md")) + ["README.md", "USER_GUIDE.md", "PACKAGING.md"]
problems = 0
for path in files:
    in_block = False
    for lineno, line in enumerate(open(path).read().split("\n"), 1):
        if line.strip().startswith("```"):
            in_block = not in_block
        if in_block:
            continue
        for target, fragment in re.findall(r"\]\(([^)#\s]*)(#[^)\s]*)?\)", line):
            if target.startswith(("http://", "https://", "mailto:")):
                continue
            dest = path if not target else os.path.normpath(os.path.join(os.path.dirname(path), target))
            if not os.path.exists(dest):
                print(f"{path}:{lineno}: missing file {target}", file=sys.stderr)
                problems += 1
            elif fragment and dest.endswith(".md") and fragment[1:] not in anchors(dest):
                print(f"{path}:{lineno}: no heading #{fragment[1:]} in {dest}", file=sys.stderr)
                problems += 1
print(f"checked links in {len(files)} files: {problems} problem(s)")
sys.exit(1 if problems else 0)
