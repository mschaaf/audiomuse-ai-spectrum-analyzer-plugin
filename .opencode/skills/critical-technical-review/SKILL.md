---
name: critical-technical-review
description: Critically brainstorm, design, validate, or reconcile a repository implementation with Claude CLI and Codex CLI. Use when the user requests Claude and Codex to investigate whether code, a proposed approach, or an implementation plan is correct.
---

# Critical Technical Review

Use `ask-claude` and `ask-codex` as separate external consultants to critically
brainstorm, design, inspect, or reconcile repository implementation work. The
subject may be an implementation plan, existing code, a proposed code change,
an architectural decision, an observed bug, or whether an approach is correct.
The user controls implementation; do not edit code merely because this review
completes.

Keep one persistent Claude session and one persistent Codex session for the
entire review. Capture each identifier on its first call and resume it for every
later round. This preserves each consultant's repository investigation, prior
proposal, findings, and accepted/rejected objections without repeatedly
reloading that context. Do not cross-resume a Claude session with Codex or vice
versa.

## Per-Round Output Contract

Every consultant turn is a file, not a chat message. Each consultant writes its
complete position, findings, and evidence for that round to a plain-text file
under `.opencode/reviews/`, named `<review-slug>-r<round>-<consultant>.txt`
(`<consultant>` is `claude` or `codex`; `<review-slug>` is a short kebab-case
label for the subject; `<round>` is the 1-based round number). Give the
consultant the exact file path to write to in its prompt.

The consultant then returns to this session ONLY two things:

- The path of the file it just wrote.
- Exactly one decision: `accept the plan` or `continue discussion`.

`accept the plan` means the consultant has no unresolved evidence-backed issues
and endorses the current position. `continue discussion` means it still has
unresolved evidence-backed issues, or is not ready to endorse the position.

The full reasoning stays in the file; the conversation carries only the path and
the decision. Do not let a consultant paste its full analysis back into the
session. When you need a consultant's reasoning to decide the next step, read the
file it just wrote rather than asking it to repeat itself.

## Process

1. Ask Claude to inspect the repository and produce an initial technical
   position for the user's request. Depending on the request, this can be a
   brainstormed design, an implementation plan, an assessment of existing code,
   or a proposed correction. Require affected files and symbols, expected
   behavior, alternatives, risks, tests, and concrete evidence for every claim.
   Per the output contract, Claude writes this to its round file and returns the
   path plus one decision.
2. Send Codex the user's request and Claude's current position (read it from
   Claude's round file). Ask it to inspect the repository independently, identify
   incorrect assumptions and omitted alternatives, and critically challenge the
   position. Require every objection or approval to cite concrete proof: paths
   and lines, reproducible commands/output, or a precise control/data-flow
   explanation. Codex must list unresolved issues separately from suggestions.
   Codex writes its review to its round file and returns the path plus one
   decision.
3. Send Claude the current position and Codex's review (read it from Codex's
   round file). Ask Claude to verify each point against the repository, revise
   its position where the evidence supports it, and explicitly accept or reject
   every finding with concrete proof. Claude writes the revised position to its
   round file and returns the path plus one decision.
4. Repeat the Codex challenge and Claude revision, each round writing a fresh
   round file and returning the path plus one decision, until both consultants'
   latest decision is `accept the plan`, or the round limit is reached.

For every resumed call, provide only the other consultant's latest response (from
its round file), the specific review/revision request, and a concise list of
repository changes since the consultant's prior turn. Do not resend the complete
plan history. If either CLI cannot resume its stored session, disclose the lost
continuity, start one replacement session for that consultant with the current
technical position and latest review, then retain and resume the replacement
identifier for all remaining rounds.

## Criticality And Bounds

- Neither consultant may stop at generic approval. They must actively look for
  correctness bugs, regressions, missing edge cases, unsafe assumptions, and
  missing tests.
- Claims without concrete proof remain unresolved rather than becoming plan
  requirements.
- A consultant returns `continue discussion` while it holds any unresolved
  evidence-backed issue; it may return `accept the plan` only when it has none.
- The loop ends when both consultants' latest decision is `accept the plan`, the
  round limit is reached, or the user terminates.
- A normal review allows at most five Claude-to-Codex-to-Claude rounds.
- If the user explicitly asks to continue after the normal limit, allow at most
  five additional rounds.
- If the user explicitly terminates the review, stop the loop immediately and
  report the latest plan and unresolved findings. Do not start another round.

## Final Output

Report the final technical position, the number of completed rounds, each
consultant's final decision, the explicit agreement status from both, and every
unresolved issue with its evidence. Point to the round files that hold the full
record. When applicable, make the final position directly actionable as an
implementation plan. If agreement was not reached by the applicable limit, say
so plainly; do not imply the design, code, or plan is validated.
