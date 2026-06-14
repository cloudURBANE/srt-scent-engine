# Project instructions

## First-time setup (run once per clone)

Activate the committed git hooks so the merge-ladder guardrail is enforced
locally:

```sh
git config core.hooksPath .githooks
```

`core.hooksPath` is local config and cannot be committed, so each fresh clone
must run this once. The hook itself lives in `.githooks/` and is tracked.

## Git workflow — READ BEFORE ANY git merge/push (hard rules)

History on this repo previously tangled into a "merge ladder": one long-lived
branch (`feat/serper-fg-discovery`) was PR'd into `main` 50+ times and `main`
was repeatedly **merged back into** the feature branch. That back-merge pulls
main's entire merge history into the feature branch and makes the graph
unreadable. Do not recreate this. The rules below are mandatory for every agent.

### Branch model: short-lived feature branches
- A feature branch exists to carry **one logical change** to `main`, then dies.
- **After a feature branch's PR is merged into `main`, that branch is done.**
  Do not keep committing to it. Start the next change on a **fresh** branch cut
  from the updated `main`:
  ```sh
  git fetch origin
  git switch main && git pull --ff-only
  git switch -c feat/<new-short-name>
  ```
- Never reuse a branch that has already been merged for unrelated new work.

### NEVER back-merge main (or any base) into a feature branch
- ❌ Forbidden: `git merge origin/main` (or `git merge main`, `git pull` that
  creates a merge) **while on a feature branch**. This is what created the
  ladder.
- ✅ If a feature branch needs to catch up to `main`, **rebase** instead:
  ```sh
  git fetch origin
  git rebase origin/main          # replays your commits on top of main; no merge node
  ```
  Only rebase branches that are **not** shared / have no open PR others review.
  If a branch is shared, prefer cutting a fresh branch over rebasing.
- Integration happens in **one direction only**: feature → `main`, via PR.
  `main` never flows back into a feature branch through a merge commit.

### Merging to main
- Land changes on `main` through a PR. Let the merge happen on the remote/PR.
- Do not run local `git merge` of a feature branch into `main` and push `main`
  directly unless the user explicitly asks for it.

### If you think you genuinely need a merge that violates the above
Stop and ask the user first. Explain why a rebase / fresh branch won't work.
Do not "just merge to make it work" — that is exactly how the ladder formed.

### Quick self-check before `git merge` or `git push`
1. Am I on a feature branch? If yes, I must not be merging `main` into it.
2. Has this branch already been merged to `main`? If yes, cut a fresh branch.
3. Is the history I'm about to create linear (rebase) rather than a back-merge?

A committed `prepare-commit-msg` hook (in `.githooks/`, activated via the
first-time-setup step above) enforces the no-back-merge rule as a hard backstop,
but a clone that skipped setup won't have it active — so these written rules are
the real guardrail.
