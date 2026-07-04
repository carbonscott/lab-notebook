#!/usr/bin/env python3
"""Minimal lab notebook: a git-tracked, append-only log you scan.

A notebook is not a database to configure. It is a directory of per-writer
JSONL files under `.lnb/`. Writing appends one JSON line; every read is a scan
of every line. No index, no schema, no config. When the notebook outgrows a
scan (~10^5 entries), the reviewable next rung is `lnb sql`, which rebuilds a
throwaway SQLite from scratch on each call -- never a persistent cache.

    lnb note "content" [#type] [@context] [k=v ...]   # the whole write path
    lnb find [terms...] [@context] [#type]            # the whole read path
    lnb retract <id> --reason "why"
    lnb sql "SELECT ...  FROM entries"                 # optional escape hatch

Discovery: $LNB_DIR, else the nearest `.lnb/` walking up from the cwd, else
`./.lnb` is created on the first `note`. Writer: $LNB_WRITER, else $USER.
"""
import glob
import json
import os
import re
import secrets
import subprocess
import sys
from datetime import datetime

TYPES_HINT = "observation, decision, dead-end, question, milestone, note"
ID_RE = re.compile(r"^[0-9A-Fa-fT-]{3,}$")  # a term that could be an id fragment


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


# --- the single load path ---------------------------------------------------

def load(nbdir):
    """Scan every .lnb/*.jsonl, drop retracted ids, return entries by ts.

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
    live = [e for e in entries if e.get("id") not in retracted]
    live.sort(key=lambda e: e.get("ts", ""))
    return live


def append(nbdir, record):
    """Append one line, all-or-nothing: one \\n-terminated write, flush + fsync."""
    os.makedirs(nbdir, exist_ok=True)
    path = os.path.join(nbdir, f"{writer_id()}.jsonl")
    line = json.dumps(record, ensure_ascii=False) + "\n"
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(line)
        fh.flush()
        os.fsync(fh.fileno())


def new_id(now):
    return now.strftime("%Y%m%dT%H%M%S") + "-" + secrets.token_hex(4)


# --- commands ---------------------------------------------------------------

def cmd_note(args):
    content, ctype, context, extras = None, "note", None, {}
    i = 0
    while i < len(args):
        a = args[i]
        if a in ("--type", "--context"):
            i += 1
            val = args[i] if i < len(args) else ""
            if a == "--type":
                ctype = val
            else:
                context = val
        elif (a.startswith("#") or a.startswith("+")) and len(a) > 1:
            ctype = a[1:]
        elif a.startswith("@") and len(a) > 1:
            context = a[1:]
        elif content is None and not a.startswith("-"):
            content = a
        elif re.match(r"^[A-Za-z_]\w*=", a):
            k, _, v = a.partition("=")
            extras[k] = v
        else:
            die(f'unexpected argument: {a!r}\n'
                f'usage: lnb note "content" [#type] [@context] [key=value ...]')
        i += 1

    if not content:
        die('nothing to log.\n'
            'usage: lnb note "content" [#type] [@context] [key=value ...]')

    now = datetime.now().astimezone()
    record = {
        "id": new_id(now),
        "ts": now.isoformat(timespec="seconds"),
        "writer": writer_id(),
        "context": context or default_context(),
        "type": ctype,
        "content": content,
    }
    record.update(extras)
    nbdir = find_notebook(create=True)
    append(nbdir, record)
    print(f'noted {record["id"]}  @{record["context"]} #{record["type"]}')
    print(f'  {content}')


def cmd_find(args):
    context = ctype = None
    terms = []
    i = 0
    while i < len(args):
        a = args[i]
        if a in ("--type", "--context"):
            i += 1
            val = args[i] if i < len(args) else ""
            if a == "--type":
                ctype = val
            else:
                context = val
        elif a.startswith("@") and len(a) > 1:
            context = a[1:]
        elif (a.startswith("#") or a.startswith("+")) and len(a) > 1:
            ctype = a[1:]
        else:
            terms.append(a)
        i += 1

    nbdir = find_notebook()
    if nbdir is None:
        die("no notebook found here. Start one with:  lnb note \"...\"", code=0)
    rows = load(nbdir)
    if not rows:
        die(f'notebook at {nbdir} is empty -- nothing logged yet.\n'
            f'Start with:  lnb note "..."', code=0)

    # A single id-ish term that uniquely matches an id -> show that entry whole.
    if len(terms) == 1 and not context and not ctype and ID_RE.match(terms[0]):
        hits = [e for e in rows if terms[0] in e.get("id", "")]
        if len(hits) == 1:
            show_entry(hits[0])
            return

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
        filt = ""
        if context:
            filt += f" @{context}"
        if ctype:
            filt += f" #{ctype}"
        q = " ".join(terms)
        contexts = len({e.get("context") for e in rows})
        print(f'no matches for "{q}"{filt} '
              f'({len(rows)} entries, {contexts} contexts).', file=sys.stderr)
        print("Fewer terms usually helps, or drop the @context/#type filter.",
              file=sys.stderr)
        return

    if not terms and not context and not ctype:
        live = live[-10:]  # default view: the 10 most recent
    print_table(live)


def cmd_retract(args):
    target, reason = None, None
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--reason":
            i += 1
            reason = args[i] if i < len(args) else None
        elif target is None:
            target = a
        i += 1
    if not target:
        die("usage: lnb retract <id> --reason \"why\"")
    if not reason:
        die("a --reason is required (recorded in the tombstone).")

    nbdir = find_notebook()
    if nbdir is None:
        die("no notebook found here.")
    rows = load(nbdir)
    hits = [e for e in rows if e.get("id") == target] or \
           [e for e in rows if target in e.get("id", "")]
    if len(hits) == 0:
        msg = f"no entry '{target}'."
        near = [e for e in rows if target.lower() in e.get("content", "").lower()]
        if near:
            e = near[0]
            msg += f' Did you mean {e["id"]} ("{e.get("content","")[:40]}...")?'
        die(msg)
    if len(hits) > 1:
        die(f"'{target}' is ambiguous -- matches {len(hits)} entries. "
            f"Use a longer id.")

    entry = hits[0]
    now = datetime.now().astimezone()
    append(nbdir, {
        "id": new_id(now),
        "ts": now.isoformat(timespec="seconds"),
        "writer": writer_id(),
        "type": "_retract",
        "retracts": entry["id"],
        "reason": reason,
    })
    print(f'retracted {entry["id"]}  ({reason})')


def cmd_sql(args):
    import sqlite3
    if not args:
        die('usage: lnb sql "SELECT ... FROM entries"')
    query = " ".join(args)
    nbdir = find_notebook()
    if nbdir is None:
        die("no notebook found here.")
    rows = load(nbdir)
    core = ("id", "ts", "writer", "context", "type", "content")
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE entries "
                "(id, ts, writer, context, type, content, extra)")
    for e in rows:
        extra = {k: v for k, v in e.items() if k not in core}
        con.execute(
            "INSERT INTO entries VALUES (?,?,?,?,?,?,?)",
            (*(e.get(c) for c in core), json.dumps(extra) if extra else None),
        )
    try:
        cur = con.execute(query)
    except sqlite3.Error as e:
        die(f"sql error: {e}")
    out = cur.fetchall()
    for row in out:
        print("\t".join("" if v is None else str(v) for v in row))


# --- rendering & helpers ----------------------------------------------------

def print_table(rows):
    for e in rows:
        content = (e.get("content") or "").replace("\n", " ")
        if len(content) > 80:
            content = content[:77] + "..."
        print(f'{e.get("id",""):<26} {e.get("ts","")[:19]:<19} '
              f'@{e.get("context",""):<16} #{e.get("type",""):<12} {content}')


def show_entry(e):
    core = ["id", "ts", "writer", "context", "type", "content"]
    width = max(len(k) for k in list(e.keys()) + core)
    for k in core:
        if k in e:
            print(f"{k:<{width}}  {e[k]}")
    for k, v in e.items():
        if k not in core:
            print(f"{k:<{width}}  {v}")


def warn(msg):
    print(f"lnb: {msg}", file=sys.stderr)


def die(msg, code=1):
    print(f"lnb: {msg}" if code else msg, file=sys.stderr)
    sys.exit(code)


USAGE = """lnb -- minimal lab notebook

  lnb note "content" [#type] [@context] [key=value ...]
  lnb find [terms...] [@context] [#type]
  lnb retract <id> --reason "why"
  lnb sql "SELECT ... FROM entries"

types are free-form; common ones: %s
""" % TYPES_HINT


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if not argv or argv[0] in ("-h", "--help", "help"):
        print(USAGE)
        return 0
    cmd, rest = argv[0], argv[1:]
    dispatch = {
        "note": cmd_note, "find": cmd_find,
        "retract": cmd_retract, "sql": cmd_sql,
    }
    if cmd not in dispatch:
        die(f"unknown command {cmd!r}.\n{USAGE}")
    dispatch[cmd](rest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
