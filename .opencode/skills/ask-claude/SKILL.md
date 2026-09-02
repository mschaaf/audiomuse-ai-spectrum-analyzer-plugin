---
name: ask-claude
description: Ask Claude CLI for repository-aware technical analysis. Use when the user asks to consult Claude, get a Claude second opinion, or have Claude investigate this repository.
---

# Ask Claude

Use the already-authenticated `claude` CLI to obtain a critical, repository-aware
consultation. Run it from the repository root so Claude can inspect the working
tree and relevant project files itself.

## Session Continuity

Keep a Claude consultation in the same Claude CLI session whenever the user is
continuing the same investigation. On the first invocation, capture the session
identifier reported by Claude and retain it in the active OpenCode conversation.
On follow-up questions, resume that identifier with Claude's non-interactive
resume option instead of starting a fresh consultation. Include only a concise
statement of the new question and any repository changes made since Claude's
last response; do not resend the full prior prompt or transcript.

Start a new Claude session only when the user explicitly requests one, the
previous session cannot be resumed, or the new request is unrelated. If resume
fails, say so, start a new session only when appropriate, and clearly state that
the previous Claude context was not retained.

Before invoking Claude, turn the user's request into a focused prompt that:

- States the question or decision to investigate.
- Says this is the active repository and Claude should inspect relevant source,
  tests, configuration, and git state before reaching conclusions.
- Requires critical findings, not reassurance.
- Requires concrete proof for every finding: file paths and line numbers,
  reproducible commands and output, or a specific control/data-flow explanation.
- Requires explicit uncertainty and test gaps when proof is unavailable.
- Prohibits making changes unless the user explicitly requested implementation.

Invoke the CLI non-interactively using `claude -p` from the repository root. For
a continuation, use Claude's session-resume capability with the retained session
identifier. Pass the complete prompt as one argument without putting secrets,
credentials, or unrelated local data into it. If the CLI is unavailable,
authentication fails, or the session cannot be resumed, report that failure and
do not substitute an invented Claude response.

Return Claude's substantive response to the user, clearly identified as external
consultation. Distinguish Claude's claims from facts independently verified in
this session.
