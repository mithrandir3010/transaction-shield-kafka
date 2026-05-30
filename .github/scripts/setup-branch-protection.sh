#!/usr/bin/env bash
# ── Branch Protection Setup ───────────────────────────────────────────────────
#
# Configures GitHub branch protection rules for `main` so that:
#   - All CI quality gates must pass before merging
#   - At least 1 PR review required
#   - Stale reviews dismissed on new commits
#   - Conversation resolution required
#   - Linear history enforced (no merge commits)
#
# Prerequisites:
#   gh auth login  (or GH_TOKEN env var)
#   Run from the repository root.
#
# Usage:
#   chmod +x .github/scripts/setup-branch-protection.sh
#   ./.github/scripts/setup-branch-protection.sh
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner)
BRANCH="main"

echo "Setting up branch protection for ${REPO}:${BRANCH} ..."

# Required status checks — must match the job 'name:' field in ci.yml exactly
REQUIRED_CHECKS=$(cat <<'EOF'
[
  "Build",
  "Unit Tests",
  "Contract Tests",
  "Integration Tests",
  "Security Scan",
  "Image Scan (transaction-producer)",
  "Image Scan (fraud-engine)",
  "Image Scan (alert-service)"
]
EOF
)

gh api \
  --method PUT \
  "repos/${REPO}/branches/${BRANCH}/protection" \
  --header "Accept: application/vnd.github+json" \
  --field "required_status_checks[strict]=true" \
  --field "required_status_checks[contexts]=$(echo $REQUIRED_CHECKS | jq -c .)" \
  --field "enforce_admins=false" \
  --field "required_pull_request_reviews[required_approving_review_count]=1" \
  --field "required_pull_request_reviews[dismiss_stale_reviews]=true" \
  --field "required_pull_request_reviews[require_conversation_resolution]=true" \
  --field "required_linear_history=true" \
  --field "allow_force_pushes=false" \
  --field "allow_deletions=false" \
  --field "restrictions=null"

echo ""
echo "✅ Branch protection applied:"
echo "   Repo   : ${REPO}"
echo "   Branch : ${BRANCH}"
echo "   Rules  : required reviews=1, linear history, all CI checks required"
echo ""
echo "Tip: after enabling, push to main only via PR."

# Create the initial v1.0.0 tag so release-please starts from a clean baseline
echo ""
echo "Tagging current HEAD as v1.0.0 (release-please baseline) ..."
git tag -a "v1.0.0" -m "chore: initial release v1.0.0" HEAD 2>/dev/null && \
  git push origin "v1.0.0" && \
  echo "✅ Tagged v1.0.0" || \
  echo "⚠️  v1.0.0 tag already exists — skipping"
