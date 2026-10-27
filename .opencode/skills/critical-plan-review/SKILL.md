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

## Process

1. Ask Claude to inspect the repository and produce an initial technical
   position for the user's request. Depending on the request, this can be a
   brainstormed design, an implementation plan, an assessment of existing code,
   or a proposed correction. Require affected files and symbols, expected
   behavior, alternatives, risks, tests, and concrete evidence for every claim.
2. Send Codex the user's request and Claude's current position. Ask it to inspect
   the repository independently, identify incorrect assumptions and omitted
   alternatives, and critically challenge the position. Require every objection
   or approval to cite concrete proof: paths and lines, reproducible
   commands/output, or a precise control/data-flow explanation. Codex must list
   unresolved issues separately from suggestions.
3. Send Claude the current position and Codex's review. Ask Claude to verify each
   point against the repository, revise its position where the evidence supports
   it, and explicitly accept or reject every finding with concrete proof.
4. Repeat the Codex challenge and Claude revision until both explicitly agree
   that the technical position has no unresolved evidence-backed issues, or the
   round limit is reached.

For every resumed call, provide only the other consultant's latest response, the
specific review/revision request, and a concise list of repository changes since
the consultant's prior turn. Do not resend the complete plan history. If either
CLI cannot resume its stored session, disclose the lost continuity, start one
replacement session for that consultant with the current technical position and
latest review, then retain and resume the replacement identifier for all
remaining rounds.

## Criticality And Bounds

- Neither consultant may stop at generic approval. They must actively look for
  correctness bugs, regressions, missing edge cases, unsafe assumptions, and
  missing tests.
- Claims without concrete proof remain unresolved rather than becoming plan
  requirements.
- A normal review allows at most five Claude-to-Codex-to-Claude rounds.
- If the user explicitly asks to continue after the normal limit, allow at most
  five additional rounds.
- If the user explicitly terminates the review, stop the loop immediately and
  report the latest plan and unresolved findings. Do not start another round.

## Final Output

Report the final technical position, the number of completed rounds, the
explicit agreement status from both consultants, and every unresolved issue with
its evidence. When applicable, make the final position directly actionable as an
implementation plan. If agreement was not reached by the applicable limit, say
so plainly; do not imply the design, code, or plan is validated.
