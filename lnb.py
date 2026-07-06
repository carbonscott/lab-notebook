#!/usr/bin/env python3
"""Minimal lab notebook: a git-tracked, append-only JSONL log. lnb is the
PRODUCER; jq is the CONSUMER.

A notebook is not a database to configure. It is a directory of per-writer
JSONL files under `.lnb/`. `note` appends one well-formed JSON line; `log`
emits every line; `retract` appends a tombstone. No index, no schema, no
config. lnb's whole job is making it easy to *write* good JSONL -- reading,
filtering, projecting and aggregating are jq's job over the stream `log` emits.

    lnb note "content" [+type] [@context] [key=value ...]   # append one record
    lnb log     # emit ALL records as JSONL, ascending by ts; jq is the read path
    lnb retract <id> --reason "why"                         # append a tombstone

`log` is a literal, argument-free emitter: it prints every record verbatim --
entries AND `_retract` tombstones -- uncapped, oldest first. It runs no filter
and no id-lookup; ALL selection, including liveness, is jq's. It exits 0 on an
empty or absent notebook (a stderr onboarding nudge only) and fails closed on
any argument. Most-recent-first is a consumer step: `| tac` or `| tail`.

    # filter by type -- jq does the selecting:
    lnb log | jq 'select(.type=="decision")'

    # the one canonical live view -- drop tombstones and the entries they retract:
    lnb log | jq -s 'map(select(.type=="_retract").retracts) as $dead
      | .[] | select(.type != "_retract" and (.id | IN($dead[]) | not))'
    # naive filters are FAIL-OPEN: `lnb log | jq 'select(.type=="decision")'`
    # returns RETRACTED decisions too -- compose the live view first.

Type defaults to "note"; context defaults to the git repo name. Both are
overridable: +type / @context sigils (shell-safe), or --type / --context flags
for scripts. (`#type` is also parsed, but `#` is the shell comment char, so it
vanishes unquoted -- prefer `+type`.) Any key=value becomes an entry field,
unvalidated: a typo'd key silently becomes a new field. The notebook trusts its
writers -- except at the write boundary, which is fail-closed (see cmd_note).

`log` sorts on the ISO ts STRING and assumes a stable UTC offset (a single-
timezone notebook); cross-offset instant ordering is a documented limitation.

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
KV_RE = re.compile(r"^[A-Za-z_][\w.-]*=\S*$")  # key=value, no whitespace
USAGE = ('usage: lnb note "content" [+type] [@context] [key=value ...]  |  '
         'log  |  retract <id> --reason "why"')


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
    """Scan every .lnb/*.jsonl -> (all records incl. `_retract` tombstones,
    ascending by ts string; the retracted id set).

    `log` wants the raw log verbatim; `retract` uses the `retracted` set to
    own its own liveness guard. Fails closed per line: a malformed/partial line
    is skipped with a warning, never crashes a read and is never rewritten.
    Sorts on the ISO ts STRING -- assumes a stable UTC offset (a single-
    timezone notebook); cross-offset instant ordering is a documented
    limitation, not a tested guarantee.
    """
    records, retracted = [], set()
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
                    if not isinstance(rec, dict):              # valid JSON, but a
                        warn(f"skipping non-object line "      # scalar/array is not
                             f"{os.path.basename(path)}:{lineno}")  # a record -- fail
                        continue                               # closed, never crash
                    records.append(rec)                        # every record, verbatim
                    if rec.get("type") == "_retract" and rec.get("retracts"):
                        retracted.add(rec["retracts"])
        except OSError as e:
            warn(f"cannot read {path}: {e}")
    records = sorted(records, key=lambda e: e.get("ts", ""))    # ascending, no reverse=
    return records, retracted


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


# --- one arg grammar, used by note ------------------------------------------

def parse(args):
    """-> (positionals, ctype, context, extras). Sigils +type/#type/@context,
    flags --type/--context, key=value -> extras, everything else positional."""
    positionals, ctype, context, extras = [], None, None, {}
    i = 0
    while i < len(args):
        a = args[i]
        if a in ("--type", "--context"):
            if i + 1 >= len(args):
                die(f"{a} needs a value.\n{USAGE}")
            i += 1
            if a == "--type":
                ctype = args[i]
            else:
                context = args[i]
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
    return positionals, ctype, context, extras


# --- commands ---------------------------------------------------------------

def cmd_note(args):
    positionals, ctype, context, extras = parse(args)
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


def cmd_log(args):
    """Emit every record as JSONL, ascending by ts -- entries AND `_retract`
    tombstones, uncapped, verbatim. No filter, no id-lookup, no diagnostics:
    all selection (including liveness) is jq's job. A stray argument is a
    fail-closed usage error, never a silent filter."""
    if args:
        die(f"log takes no arguments -- it emits every record; filter with jq.\n{USAGE}")
    nbdir = find_notebook()
    if nbdir is None:
        print('no notebook found here. Start one with:  lnb note "..."',
              file=sys.stderr)
        return
    records, _ = scan(nbdir)
    if not records:
        print(f'notebook at {nbdir} is empty -- nothing logged yet.\n'
              f'Start with:  lnb note "..."', file=sys.stderr)
        return
    emit_json(records)


def cmd_retract(args):
    target, reason = None, None
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--reason":
            i += 1
            reason = args[i] if i < len(args) else None
        elif a in ("-o", "--output"):
            die("retract has no -o/--output; it takes an <id> and --reason only. "
                "To read records, use `lnb log`.")
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
    records, retracted = scan(nbdir)
    # scan() now returns tombstones and dead entries too, so retract must own
    # its liveness: only REAL, still-live entries are retractable candidates.
    # Both guards matter -- `type != "_retract"` stops a substring `target`
    # from matching a tombstone's own id ("retract a tombstone"); `id not in
    # retracted` stops a second tombstone for an already-dead id.
    live = [e for e in records
            if e.get("type") != "_retract" and e.get("id") not in retracted]
    hits = [e for e in live if e.get("id") == target] or \
           [e for e in live if target in e.get("id", "")]
    if len(hits) > 1:
        die(f"'{target}' is ambiguous -- matches {len(hits)}: "
            f"{', '.join(e['id'] for e in hits)}. Use a longer id.")
    if not hits:
        if any(target == r or target in r for r in retracted):
            die(f"'{target}' is already retracted.")
        die(f"no entry '{target}'.{suggest_id(target, live)}")

    entry = hits[0]
    append(nbdir, new_record(datetime.now().astimezone(),
                             type="_retract", retracts=entry["id"], reason=reason))
    print(f'retracted {entry["id"]}  ({reason})')


# --- rendering, diagnostics & helpers ---------------------------------------

def emit_json(rows):
    for e in rows:                      # JSONL: one verbatim record per line
        print(json.dumps(e, ensure_ascii=False))


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
    dispatch = {"note": cmd_note, "log": cmd_log,
                "retract": cmd_retract}
    if cmd not in dispatch:
        die(f"unknown command {cmd!r}.\n{USAGE}")
    dispatch[cmd](rest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
