#!/usr/bin/env python3
"""Minimal lab notebook: a git-tracked, append-only log you scan.

A notebook is not a database to configure. It is a directory of per-writer
JSONL files under `.lnb/`. Writing appends one JSON line; every read is a scan
of every line. No index, no schema, no config. When the notebook outgrows a
scan (~10^5 entries), the reviewable next rung is `lnb sql`, which rebuilds a
throwaway SQLite from scratch on each call -- never a persistent cache.

    lnb note "content" [+type] [@context] [key=value ...]   # the whole write path
    lnb find [terms...] [@context] [+type]                  # the whole read path
    lnb retract <id> --reason "why"
    lnb sql "SELECT ... FROM entries"                        # optional escape hatch

Type defaults to "note"; context defaults to the git repo name. Both are
overridable: +type / @context sigils (shell-safe), or --type / --context flags
for scripts. (`#type` is also parsed, but `#` is the shell comment char, so it
vanishes unquoted -- prefer `+type`.) Any key=value becomes an entry field,
unvalidated: a typo'd key silently becomes a new field. The notebook trusts its
writers -- except at the write boundary, which is fail-closed (see cmd_note).

Discovery: $LNB_DIR, else the nearest `.lnb/` walking up from the cwd, else
`./.lnb` is created on the first `note`. Writer: $LNB_WRITER, else $USER.
"""
import difflib
import glob
import json
import os
import re
import secrets
import subprocess
import sys
from datetime import datetime

CORE = ("id", "ts", "writer", "context", "type", "content")  # lnb sets these
RESERVED = set(CORE) | {"retracts", "reason"}                # writers may not
# An id fragment: hex/timestamp charset AND at least one digit, so plain
# hex-lookalike words ("cafe", "dead") stay content searches, not id lookups.
ID_RE = re.compile(r"^(?=.*\d)[0-9A-Fa-fT:+-]{3,}$")
KV_RE = re.compile(r"^[A-Za-z_][\w.-]*=\S*$")  # key=value, no whitespace
USAGE = ('usage: lnb note "content" [+type] [@context] [key=value ...]  |  '
         'find [terms] [@ctx] [+type]  |  retract <id> --reason "why"  |  '
         'sql "SELECT ... FROM entries"')


# --- notebook discovery & identity ------------------------------------------

def find_notebook(create=False):
    """Return the notebook dir. $LNB_DIR wins; else nearest .lnb/ walking up."""
    env = os.environ.get("LNB_DIR")
    if env:
        if create:
            os.makedirs(env, exist_ok=True)
        return env
    cur = os.getcwd()
    while True:
        cand = os.path.join(cur, ".lnb")
        if os.path.isdir(cand):
            return cand
        parent = os.path.dirname(cur)
        if parent == cur:
            break
        cur = parent
    if create:
        d = os.path.join(os.getcwd(), ".lnb")
        os.makedirs(d, exist_ok=True)
        return d
    return None


def writer_id():
    return os.environ.get("LNB_WRITER") or os.environ.get("USER") or "anon"


def default_context():
    """The enclosing git repo's name, else the cwd basename."""
    try:
        top = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True, text=True, timeout=2,
        )
        if top.returncode == 0 and top.stdout.strip():
            return os.path.basename(top.stdout.strip())
    except (OSError, subprocess.SubprocessError):
        pass
    return os.path.basename(os.getcwd()) or "notebook"


# --- the single read path & the single write path ---------------------------

def scan(nbdir):
    """Scan every .lnb/*.jsonl -> (live entries by ts, retracted id set).

    Fails closed per line: a malformed/partial line is skipped with a warning,
    never crashes a read and is never rewritten.
    """
    entries, retracted = [], set()
    for path in sorted(glob.glob(os.path.join(nbdir, "*.jsonl"))):
        try:
            with open(path, encoding="utf-8") as fh:
                for lineno, line in enumerate(fh, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        warn(f"skipping malformed line {os.path.basename(path)}:{lineno}")
                        continue
                    if rec.get("type") == "_retract":
                        if rec.get("retracts"):
                            retracted.add(rec["retracts"])
                    else:
                        entries.append(rec)
        except OSError as e:
            warn(f"cannot read {path}: {e}")
    live = sorted((e for e in entries if e.get("id") not in retracted),
                  key=lambda e: e.get("ts", ""))
    return live, retracted


def append(nbdir, record):
    """Append one line, all-or-nothing: one \\n-terminated write, flush + fsync."""
    os.makedirs(nbdir, exist_ok=True)
    path = os.path.join(nbdir, f"{writer_id()}.jsonl")
    line = json.dumps(record, ensure_ascii=False) + "\n"
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(line)
        fh.flush()
        os.fsync(fh.fileno())


def new_record(now, **fields):
    rec = {"id": now.strftime("%Y%m%dT%H%M%S") + "-" + secrets.token_hex(4),
           "ts": now.isoformat(timespec="seconds"),
           "writer": writer_id()}
    rec.update(fields)
    return rec


# --- one arg grammar, shared by note & find ---------------------------------

def parse(args):
    """-> (positionals, ctype, context, extras, output). Sigils +type/#type/@context,
    flags --type/--context/-o/--output, key=value -> extras, everything else positional."""
    positionals, ctype, context, extras, output = [], None, None, {}, None
    i = 0
    while i < len(args):
        a = args[i]
        if a in ("--type", "--context", "-o", "--output"):
            if i + 1 >= len(args):
                die(f"{a} needs a value.\n{USAGE}")
            i += 1
            if a == "--type":
                ctype = args[i]
            elif a == "--context":
                context = args[i]
            else:
                output = args[i]
        elif a[:1] in "+#" and len(a) > 1:
            ctype = a[1:]
        elif a[:1] == "@" and len(a) > 1:
            context = a[1:]
        elif KV_RE.match(a):           # key=value BEFORE positional, so a stray
            k, _, v = a.partition("=")  # `note tags=x` (no content) fails loudly
            extras[k] = v
        elif not a.startswith("-"):
            positionals.append(a)
        else:
            die(f"unexpected argument: {a!r}\n{USAGE}")
        i += 1
    return positionals, ctype, context, extras, output


# --- commands ---------------------------------------------------------------

def cmd_note(args):
    positionals, ctype, context, extras, output = parse(args)
    if output is not None:
        die("note has no -o/--output; use `find <id> -o json` to fetch a record as JSON")
    content = " ".join(positionals)
    if not content:
        die(f'nothing to log.\n{USAGE}')
    # Fail-closed write boundary: a writer may not forge the fields lnb owns,
    # nor the `_` namespace -- otherwise `note "x" type=_retract retracts=<id>`
    # would append a record that reads back as a tombstone and delete an entry.
    bad = sorted(k for k in extras if k in RESERVED or k.startswith("_"))
    if bad:
        die(f"reserved field name(s): {', '.join(bad)} -- lnb sets these, not you.")
    if (ctype or "").startswith("_"):
        die("type may not start with '_' (reserved for system records).")

    defaulted = ctype is None
    record = new_record(datetime.now().astimezone(),
                        context=context or default_context(),
                        type=ctype or "note", content=content, **extras)
    nbdir = find_notebook(create=True)
    append(nbdir, record)
    flag = " (default)" if defaulted else ""
    print(f'noted {record["id"]}  @{record["context"]} +{record["type"]}{flag}')
    print(f'  {content}')


def cmd_find(args):
    terms, ctype, context, _, output = parse(args)
    if output is not None and output != "json":
        die(f"unknown output format {output!r} -- the only format is: json")
    as_json = output == "json"
    emit = emit_json if as_json else print_table
    nbdir = find_notebook()
    if nbdir is None:
        print('no notebook found here. Start one with:  lnb note "..."',
              file=sys.stderr)
        return
    rows, _ = scan(nbdir)
    if not rows:
        print(f'notebook at {nbdir} is empty -- nothing logged yet.\n'
              f'Start with:  lnb note "..."', file=sys.stderr)
        return

    # A single id-ish term -> that entry in full (unique), or list the matches.
    if len(terms) == 1 and not context and not ctype and ID_RE.match(terms[0]):
        hits = [e for e in rows if terms[0] in e.get("id", "")]
        if len(hits) == 1:
            return emit_json(hits) if as_json else show_entry(hits[0])
        if len(hits) > 1:
            warn(f"'{terms[0]}' matches {len(hits)} ids:")
            return emit(hits)
        # 0 id hits -> fall through and treat it as a content search

    live = rows
    if context:
        live = [e for e in live if e.get("context") == context]
    if ctype:
        live = [e for e in live if e.get("type") == ctype]
    if terms:
        query = " ".join(terms)
        try:
            pat = re.compile(query, re.IGNORECASE)
            live = [e for e in live if pat.search(e.get("content", ""))]
        except re.error:
            q = query.lower()
            live = [e for e in live if q in e.get("content", "").lower()]

    if not live:
        return no_matches(rows, terms, context, ctype)
    if not terms and not context and not ctype:
        live = live[-10:]  # default view: 10 most recent, oldest->newest
    emit(live)


def cmd_retract(args):
    target, reason = None, None
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--reason":
            i += 1
            reason = args[i] if i < len(args) else None
        elif a in ("-o", "--output"):
            die("retract has no -o/--output; use `find <id> -o json` to fetch a record as JSON")
        elif target is None:
            target = a
        i += 1
    if not target:
        die(f'usage: lnb retract <id> --reason "why"')
    if not reason:
        die("a --reason is required (recorded in the tombstone).")

    nbdir = find_notebook()
    if nbdir is None:
        die('no notebook here.  (nothing to retract)')
    rows, retracted = scan(nbdir)
    hits = [e for e in rows if e.get("id") == target] or \
           [e for e in rows if target in e.get("id", "")]
    if len(hits) > 1:
        die(f"'{target}' is ambiguous -- matches {len(hits)}: "
            f"{', '.join(e['id'] for e in hits)}. Use a longer id.")
    if not hits:
        if any(target == r or target in r for r in retracted):
            die(f"'{target}' is already retracted.")
        die(f"no entry '{target}'.{suggest_id(target, rows)}")

    entry = hits[0]
    append(nbdir, new_record(datetime.now().astimezone(),
                             type="_retract", retracts=entry["id"], reason=reason))
    print(f'retracted {entry["id"]}  ({reason})')


def cmd_sql(args):
    import sqlite3
    if not args:
        die('usage: lnb sql "SELECT ... FROM entries"')
    nbdir = find_notebook()
    if nbdir is None:
        die('no notebook here.')
    rows, _ = scan(nbdir)
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE entries (id, ts, writer, context, type, content, extra)")
    for e in rows:
        extra = {k: v for k, v in e.items() if k not in CORE}
        con.execute("INSERT INTO entries VALUES (?,?,?,?,?,?,?)",
                    (*(e.get(c) for c in CORE), json.dumps(extra) if extra else None))
    try:
        cur = con.execute(" ".join(args))
    except sqlite3.Error as e:
        die(f"sql error: {e}")
    for row in cur.fetchall():
        print("\t".join("" if v is None else str(v) for v in row))


# --- rendering, diagnostics & helpers ---------------------------------------

def emit_json(rows):
    for e in rows:                      # JSONL: one verbatim record per line
        print(json.dumps(e, ensure_ascii=False))


def print_table(rows):
    for e in rows:
        content = (e.get("content") or "").replace("\n", " ")
        if len(content) > 80:
            content = content[:77] + "..."
        print(f'{e.get("id",""):<26} {e.get("ts","")[:19]:<19} '
              f'@{e.get("context",""):<16} +{e.get("type",""):<12} {content}')


def show_entry(e):
    width = max(len(k) for k in list(e.keys()) + list(CORE))
    for k in CORE:
        if k in e:
            print(f"{k:<{width}}  {e[k]}")
    for k, v in e.items():
        if k not in CORE:
            print(f"{k:<{width}}  {v}")


def no_matches(rows, terms, context, ctype):
    """Diagnose -> hypothesize -> prescribe (Conway): say what DOES exist."""
    q = " ".join(terms)
    filt = (f" @{context}" if context else "") + (f" +{ctype}" if ctype else "")
    print(f'no matches for "{q}"{filt} '
          f'({len(rows)} entries, {len({e.get("context") for e in rows})} contexts).',
          file=sys.stderr)
    if context and not any(e.get("context") == context for e in rows):
        print(f"  contexts present: {vocab(rows, 'context')}", file=sys.stderr)
    if ctype and not any(e.get("type") == ctype for e in rows):
        print(f"  types present: {vocab(rows, 'type')}", file=sys.stderr)
    print("Fewer terms usually helps, or drop the @context/+type filter.",
          file=sys.stderr)


def vocab(rows, field):
    return ", ".join(sorted({str(e.get(field)) for e in rows if e.get(field)}))


def suggest_id(target, rows):
    ids = [e["id"] for e in rows]
    suffixes = {i.split("-")[-1]: i for i in ids}
    near = difflib.get_close_matches(target, ids + list(suffixes), n=1, cutoff=0.4)
    if not near:
        return ""
    full = suffixes.get(near[0], near[0])
    snippet = next((e.get("content", "") for e in rows if e["id"] == full), "")
    return f' Did you mean {full} ("{snippet[:40]}")?'


def warn(msg):
    print(f"lnb: {msg}", file=sys.stderr)


def die(msg):
    print(f"lnb: {msg}", file=sys.stderr)
    sys.exit(1)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if not argv or argv[0] in ("-h", "--help", "help"):
        print(__doc__)
        return 0
    cmd, rest = argv[0], argv[1:]
    dispatch = {"note": cmd_note, "find": cmd_find,
                "retract": cmd_retract, "sql": cmd_sql}
    if cmd not in dispatch:
        die(f"unknown command {cmd!r}.\n{USAGE}")
    dispatch[cmd](rest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
