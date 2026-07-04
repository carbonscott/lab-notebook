# Iteration 2 — build notes & carry-forward for Iteration 3

Built `lnb.py` (single file, stdlib only) + minimal `SKILL.md`, `README.md`,
`tests/`. Smoke test passed for note/find/retract/sql/id-prefix/empty-notebook.

## Findings to resolve in Iteration 3 (persona review + fix)

1. **`#type` sigil collides with the shell comment char (bug, highest priority).**
   `lnb note "..." #decision` has `#decision` stripped by bash before `lnb` runs,
   so the type silently falls back to `note`. A silently-wrong append violates the
   fail-closed / "don't guess when guessing writes" principle. Candidate fix: swap
   the type sigil `#` -> `+` (`+decision`), which is not a shell metacharacter and
   pairs cleanly with `@context`. Keep `--type` as the explicit form. (`@context`
   is shell-safe, keep it.)

2. **SLOC ~302 vs the ≤250 budget.** Over the hard cap. Trim in iter3 (compress:
   dead args, the `--reason` branch inside `cmd_note`, help text). Geohot's budget
   is a forcing function, not aspirational.

3. **`cmd_note` accepts `--reason`** (copied from the flag list) but ignores it —
   dead code; drop it.

4. **Type inference** (Conway's keyword table) is deliberately deferred. If added,
   it must be echoed ("recorded as #decision (inferred)"), never silent.

5. Decide whether `sql` truly earns its place (2/3 personas wanted it out of core).
