---
name: ask-codex
description: Ask Codex CLI for repository-aware technical analysis. Use when the user asks to consult Codex, get a Codex second opinion, or have Codex investigate this repository.
---

# Ask Codex

Use the already-authenticated `codex` CLI to obtain a critical, repository-aware
consultation. Run it from the repository root so Codex can inspect the working
tree and relevant project files itself.

## Session Continuity

Keep a Codex consultation in the same Codex CLI session whenever the user is
continuing the same investigation. On the first invocation, capture the session
identifier reported by Codex and retain it in the active OpenCode conversation.
On follow-up questions, resume that identifier with Codex's non-interactive
resume command instead of starting a fresh consultation. Include only a concise
statement of the new question and any repository changes made since Codex's last
response; do not resend the full prior prompt or transcript.

Start a new Codex session only when the user explicitly requests one, the
previous session cannot be resumed, or the new request is unrelated. If resume
fails, say so, start a new session only when appropriate, and clearly state that
the previous Codex context was not retained.

Before invoking Codex, turn the user's request into a focused prompt that:

- States the question or decision to investigate.
- Says this is the active repository and Codex should inspect relevant source,
  tests, configuration, and git state before reaching conclusions.
- Requires critical findings, not reassurance.
- Requires concrete proof for every finding: file paths and line numbers,
  reproducible commands and output, or a specific control/data-flow explanation.
- Requires explicit uncertainty and test gaps when proof is unavailable.
- Prohibits making changes unless the user explicitly requested implementation.

Invoke the CLI non-interactively using `codex exec` from the repository root in
`workspace-write` sandbox mode: pass `-s workspace-write` so Codex can write
files (scratch output, test fixtures) to the current repository and /tmp, and
nothing else. This is the least-privilege write mode — it does not grant access
to the rest of the filesystem. For a continuation, use Codex's non-interactive
session-resume command with the retained session identifier; the resume
subcommand does not accept `-s` and inherits the same `workspace-write` default,
so no extra flag is needed. Pass the complete prompt as one argument without
putting secrets, credentials, or unrelated local data into it. If the CLI is
unavailable, authentication fails, or the session cannot be resumed, report that
failure and do not substitute an invented Codex response.

Prefer `workspace-write` over `--dangerously-bypass-approvals-and-sandbox`,
which drops the sandbox and skips all approval prompts entirely (unrestricted
filesystem and network access). For pure read-only inspection, use
`-s read-only` instead.

Return Codex's substantive response to the user, clearly identified as external
consultation. Distinguish Codex's claims from facts independently verified in
this session.
