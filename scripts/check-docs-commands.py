#!/usr/bin/env python3
"""Fail if the documentation shows a `timevault` command that does not exist.

Every `timevault ...` command in a fenced code block or in inline code is checked
against the real `--help` output: the subcommand (`backup`, `disk mount`, ...) and
every `--option`. Run after building; TIMEVAULT_BIN overrides the binary.
"""
import glob
import os
import re
import subprocess
import sys

binary = os.environ.get("TIMEVAULT_BIN", "target/debug/timevault")
help_text = subprocess.run([binary, "--help"], capture_output=True, text=True).stdout
if "Usage" not in help_text:
    sys.exit("could not read --help from " + binary)

options = set(re.findall(r"--[a-z][a-z0-9-]*", help_text))
options |= {"--help", "--version"}
subcommands = set(re.findall(r"timevault (disk [a-z-]+|backup|wake)", help_text))
# Aliases (for example `disk de-register`) are in the source, not in --help.
if os.path.exists("src/cli/args.rs"):
    subcommands |= {f"disk {a}" for a in re.findall(r'alias = "([a-z-]+)"', open("src/cli/args.rs").read())}
# Options that take a value: the value is not a subcommand.
takes_value = {
    "--job", "--config", "--disk-id", "--fs-uuid", "--device", "--label",
    "--mount-options", "--min-session-seconds", "--new-id", "--block-id",
}
top_level = {"backup", "wake", "disk"}
# `-h`/`--help` style flags and the placeholders used in prose.
ignore_options = {"--"}

files = sorted(glob.glob("docs/*.md")) + ["README.md", "USER_GUIDE.md", "PACKAGING.md"]
skip_files = {"docs/PLAN.md", "docs/IMPLEMENTATION_LOG.md", "docs/IMPLEMENTATION_SUMMARY.md", "docs/testing.md"}

problems = []


def check(path, lineno, command):
    words = command.split()
    # drop `sudo`, environment assignments and a leading path
    while words and (words[0] == "sudo" or "=" in words[0]):
        words = words[1:]
    if not words or os.path.basename(words[0]) != "timevault":
        return
    rest = words[1:]
    flags = []
    positional = []
    skip = False
    for w in rest:
        if skip:
            skip = False
            continue
        if w.startswith("--"):
            flag = re.match(r"--[a-z][a-z0-9-]*", w)
            flags.append(flag.group(0) if flag else w)
            skip = flags[-1] in takes_value and "=" not in w
        elif not w.startswith("-"):
            positional.append(w)
    if positional and positional[0] in top_level or not positional:
        if positional:
            sub = positional[0]
            if sub == "disk":
                if len(positional) < 2:
                    problems.append((path, lineno, command, "disk needs a subcommand"))
                elif f"disk {positional[1]}" not in subcommands:
                    problems.append((path, lineno, command, f"unknown subcommand: disk {positional[1]}"))
    elif positional and not positional[0].startswith(("<", "[", "$", "-")):
        problems.append((path, lineno, command, f"unknown subcommand: {positional[0]}"))
    for flag in flags:
        if flag not in options and flag not in ignore_options:
            problems.append((path, lineno, command, f"unknown option: {flag}"))


for path in files:
    if path in skip_files or not os.path.exists(path):
        continue
    in_block = False
    for lineno, line in enumerate(open(path).read().split("\n"), 1):
        if line.strip().startswith("```"):
            in_block = not in_block
            continue
        if in_block:
            stripped = line.strip().lstrip("$ ").split("#")[0].strip()
            if re.match(r"(sudo\s+)?(\S*/)?timevault\b", stripped):
                check(path, lineno, stripped)
        else:
            for code in re.findall(r"`([^`]*\btimevault\b[^`]*)`", line):
                if re.match(r"\s*(sudo\s+)?(\S*/)?timevault\b", code):
                    check(path, lineno, code.strip())

for path, lineno, command, why in problems:
    print(f"{path}:{lineno}: {why}\n    {command}", file=sys.stderr)
print(f"checked docs against {binary}: {len(problems)} problem(s)")
sys.exit(1 if problems else 0)
