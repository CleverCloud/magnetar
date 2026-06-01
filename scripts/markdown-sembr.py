#!/usr/bin/env python3
"""Semantic line breaks for Markdown — one sentence per line.

Joins prose that was hard-wrapped at a column limit, then re-splits at
sentence boundaries so each sentence starts on its own line. Designed to
pair with Prettier configured as ``proseWrap: preserve`` + a very high
``printWidth`` (see ``.prettierrc.json``).

What it touches:
  - Plain paragraphs (top-level prose)
  - List items (``-`` / ``*`` / ``+`` / ``N.``) — marker and indentation
    preserved; the item's prose is sentence-split
  - Blockquotes (``> ``) — prefix preserved; quoted prose sentence-split

What it leaves alone:
  - Fenced code blocks (``` and ~~~)
  - YAML frontmatter (top-of-file ``---`` / ``---``)
  - Indented code blocks (4-space, after a blank line)
  - Tables, headings, horizontal rules, HTML blocks, reference link defs
  - Anything inside inline code spans, links, or HTML
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ABBREVS = [
    "e.g.", "i.e.", "cf.", "etc.", "vs.", "viz.", "al.", "approx.", "ca.",
    "Mr.", "Mrs.", "Ms.", "Dr.", "Prof.", "Jr.", "Sr.",
    "Inc.", "Ltd.", "Co.", "St.", "Ave.",
    "U.S.", "U.K.", "E.U.", "D.C.",
    "a.m.", "p.m.", "A.M.", "P.M.",
    "Ph.D.", "M.D.", "B.S.", "B.A.", "M.S.", "M.A.",
    "No.", "Vol.", "Ch.", "Fig.", "Eq.", "Sec.",
]

SENTENCE_END_RE = re.compile(
    r"(?<=[A-Za-z0-9\)\]\"'`])"
    r"([.!?]+[\"'\)\]]?)"
    r"(\s+)"
    r"(?=[A-Z\[\(\"'`*_])"
)

HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s")
TABLE_RE = re.compile(r"^\s{0,3}\|")
HR_RE = re.compile(r"^\s{0,3}(?:-{3,}|\*{3,}|_{3,})\s*$")
HTML_BLOCK_RE = re.compile(r"^\s{0,3}<")
REFERENCE_LINK_RE = re.compile(r"^\s{0,3}\[[^\]]+\]:\s")
CODE_FENCE_RE = re.compile(r"^(\s{0,3})(```+|~~~+)")
BLANK_RE = re.compile(r"^\s*$")
LIST_MARKER_RE = re.compile(r"^(\s*)([-*+]\s+|\d+[.)]\s+)(.*)$")
BLOCKQUOTE_RE = re.compile(r"^(\s{0,3}>\s?)(.*)$")
INDENTED_CODE_RE = re.compile(r"^( {4,}|\t)")


def split_sentences(text: str) -> list[str]:
    """Split text into sentences, protecting known abbreviations."""
    placeholders: dict[str, str] = {}
    for i, abbr in enumerate(sorted(set(ABBREVS), key=len, reverse=True)):
        if abbr in text:
            ph = f"\x00ABBR{i:03d}\x00"
            placeholders[ph] = abbr
            text = text.replace(abbr, ph)

    parts = SENTENCE_END_RE.split(text)
    # parts = [text, punct, ws, text, punct, ws, ..., text]
    sentences: list[str] = []
    i = 0
    while i + 2 < len(parts):
        sentences.append(parts[i] + parts[i + 1])
        i += 3
    sentences.append(parts[i])

    for ph, abbr in placeholders.items():
        sentences = [s.replace(ph, abbr) for s in sentences]

    return [s.strip() for s in sentences if s.strip()]


def is_pass_through(line: str) -> bool:
    return bool(
        HEADING_RE.match(line)
        or TABLE_RE.match(line)
        or HR_RE.match(line)
        or HTML_BLOCK_RE.match(line)
        or REFERENCE_LINK_RE.match(line)
    )


def process(text: str) -> str:
    lines = text.splitlines()
    out: list[str] = []
    n = len(lines)
    i = 0
    in_fence = False
    fence_marker = ""

    # YAML frontmatter at top
    if n > 0 and lines[0].rstrip() == "---":
        out.append(lines[0])
        i = 1
        while i < n and lines[i].rstrip() != "---":
            out.append(lines[i])
            i += 1
        if i < n:
            out.append(lines[i])
            i += 1

    while i < n:
        line = lines[i]

        if in_fence:
            out.append(line)
            if line.lstrip().startswith(fence_marker):
                in_fence = False
            i += 1
            continue

        m = CODE_FENCE_RE.match(line)
        if m:
            fence_marker = m.group(2)[0] * 3
            in_fence = True
            out.append(line)
            i += 1
            continue

        if BLANK_RE.match(line):
            out.append(line)
            i += 1
            continue

        if is_pass_through(line):
            out.append(line)
            i += 1
            continue

        # Indented code block: only at block start (preceded by blank line
        # or document start) and not inside a list (we handle list
        # continuation explicitly).
        if INDENTED_CODE_RE.match(line) and (i == 0 or BLANK_RE.match(lines[i - 1])):
            out.append(line)
            i += 1
            continue

        # Blockquote
        if BLOCKQUOTE_RE.match(line):
            prefix = ""
            buf: list[str] = []
            while i < n:
                bq = BLOCKQUOTE_RE.match(lines[i])
                if not bq:
                    break
                if not prefix:
                    prefix = bq.group(1)
                content = bq.group(2)
                if content.strip() == "":
                    if buf:
                        for s in split_sentences(" ".join(b.strip() for b in buf)):
                            out.append(prefix + s)
                        buf = []
                    out.append(lines[i])
                    i += 1
                    continue
                buf.append(content)
                i += 1
            if buf:
                for s in split_sentences(" ".join(b.strip() for b in buf)):
                    out.append(prefix + s)
            continue

        # List item
        lm = LIST_MARKER_RE.match(line)
        if lm:
            indent = lm.group(1)
            marker = lm.group(2)
            first = lm.group(3)
            cont_indent = indent + " " * len(marker)
            buf = [first]
            i += 1
            while i < n:
                nxt = lines[i]
                if BLANK_RE.match(nxt):
                    break
                if CODE_FENCE_RE.match(nxt):
                    break
                if LIST_MARKER_RE.match(nxt):
                    break
                if is_pass_through(nxt):
                    break
                if BLOCKQUOTE_RE.match(nxt):
                    break
                # Continuation must be indented at least to cont_indent
                if nxt.startswith(cont_indent):
                    buf.append(nxt[len(cont_indent):])
                    i += 1
                elif nxt.startswith(" ") and len(nxt) - len(nxt.lstrip(" ")) >= len(indent) + 1:
                    # Lazy continuation (any indentation)
                    buf.append(nxt.lstrip())
                    i += 1
                else:
                    break
            joined = " ".join(b.strip() for b in buf if b.strip())
            sentences = split_sentences(joined)
            if sentences:
                out.append(indent + marker + sentences[0])
                for s in sentences[1:]:
                    out.append(cont_indent + s)
            else:
                out.append(line)
            continue

        # Plain paragraph
        buf = [line]
        i += 1
        while i < n:
            nxt = lines[i]
            if BLANK_RE.match(nxt):
                break
            if CODE_FENCE_RE.match(nxt):
                break
            if is_pass_through(nxt):
                break
            if BLOCKQUOTE_RE.match(nxt):
                break
            if LIST_MARKER_RE.match(nxt):
                break
            buf.append(nxt)
            i += 1
        joined = " ".join(b.strip() for b in buf if b.strip())
        for s in split_sentences(joined):
            out.append(s)

    trailing_nl = "\n" if text.endswith("\n") else ""
    return "\n".join(out) + trailing_nl


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("usage: markdown-sembr.py <file> [<file> ...]", file=sys.stderr)
        return 2
    changed = 0
    for arg in argv[1:]:
        p = Path(arg)
        original = p.read_text(encoding="utf-8")
        new = process(original)
        if new != original:
            p.write_text(new, encoding="utf-8")
            changed += 1
            print(f"reformatted: {p}")
    print(f"{changed} file(s) changed", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
