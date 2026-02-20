#!/bin/bash
# =============================================================================
# update.sh — Update ARNy_Plotter to the latest version from GitHub
#
# Usage:
#   bash scripts/update.sh              # update and restart services
#   bash scripts/update.sh --no-restart # update code only, keep services up
#   bash scripts/update.sh --dry-run    # show what would change, do nothing
#
# Repository: https://github.com/louismeuret/ARNy_Plotter
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REPO_URL="https://github.com/louismeuret/ARNy_Plotter"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
REQUIREMENTS_FILE="$PROJECT_DIR/scripts/installation/requirements.txt"
START_SCRIPT="$SCRIPT_DIR/start_services.sh"
STOP_SCRIPT="$SCRIPT_DIR/stop_services.sh"

# ---------------------------------------------------------------------------
# Colour helpers
# ---------------------------------------------------------------------------
_tty() { [ -t 1 ] && echo true || echo false; }
USE_COLOR=$(_tty)

color() {
    local code="$1"; shift
    if [ "$USE_COLOR" = "true" ]; then
        echo -e "\033[${code}m$*\033[0m"
    else
        echo "$*"
    fi
}

green()  { color "32;1" "$*"; }
red()    { color "31;1" "$*"; }
yellow() { color "33;1" "$*"; }
blue()   { color "34;1" "$*"; }
bold()   { color "1"    "$*"; }

info()    { echo "$(blue "  [INFO ]") $*"; }
success() { echo "$(green "  [OK   ]") $*"; }
warn()    { echo "$(yellow "  [WARN ]") $*"; }
error()   { echo "$(red   "  [ERROR]") $*" >&2; }
step()    { echo; bold "── $* ──"; }

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
NO_RESTART=false
DRY_RUN=false

for arg in "$@"; do
    case "$arg" in
        --no-restart) NO_RESTART=true ;;
        --dry-run)    DRY_RUN=true ;;
        -h|--help)
            echo "Usage: bash scripts/update.sh [--no-restart] [--dry-run]"
            echo ""
            echo "  --no-restart   Pull latest code but do not restart services"
            echo "  --dry-run      Show pending changes without applying them"
            exit 0
            ;;
        *)
            error "Unknown option: $arg  (use --help for usage)"
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------
echo
bold "============================================================"
bold "  ARNy_Plotter Updater"
bold "============================================================"
info "Repository : $REPO_URL"
info "Project    : $PROJECT_DIR"
[ "$DRY_RUN"    = true ] && warn "Mode       : DRY RUN — no changes will be applied"
[ "$NO_RESTART" = true ] && info "Mode       : --no-restart — services will NOT be restarted"
bold "============================================================"

# ---------------------------------------------------------------------------
# 1. Verify we are inside a git repository
# ---------------------------------------------------------------------------
step "Verifying git repository"

cd "$PROJECT_DIR"

if ! git rev-parse --is-inside-work-tree &>/dev/null; then
    error "Not inside a git repository: $PROJECT_DIR"
    error "Please run this script from the ARNy_Plotter installation directory."
    exit 1
fi

REMOTE_URL=$(git remote get-url origin 2>/dev/null || echo "")
if [ -z "$REMOTE_URL" ]; then
    error "No git remote named 'origin' found."
    error "To fix: git remote add origin $REPO_URL"
    exit 1
fi
success "Git remote: $REMOTE_URL"

# ---------------------------------------------------------------------------
# 2. Check for uncommitted local changes
# ---------------------------------------------------------------------------
step "Checking for local changes"

DIRTY_FILES=$(git status --porcelain 2>/dev/null | grep -v '^??' || true)
if [ -n "$DIRTY_FILES" ]; then
    warn "You have uncommitted local changes:"
    git status --short | grep -v '^??' | while read -r line; do
        echo "      $line"
    done
    warn "These will be preserved (git stash will NOT be run automatically)."
    warn "If the update fails, stash your changes first: git stash"
fi

# ---------------------------------------------------------------------------
# 3. Fetch and show pending changes (always, even in dry-run)
# ---------------------------------------------------------------------------
step "Fetching latest changes from GitHub"

if [ "$DRY_RUN" = false ]; then
    git fetch origin --quiet
fi

CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
CURRENT_SHA=$(git rev-parse --short HEAD)
REMOTE_SHA=$(git rev-parse --short "origin/$CURRENT_BRANCH" 2>/dev/null || echo "unknown")

info "Branch     : $CURRENT_BRANCH"
info "Local SHA  : $CURRENT_SHA"
info "Remote SHA : $REMOTE_SHA"

if [ "$CURRENT_SHA" = "$REMOTE_SHA" ]; then
    success "Already up to date — nothing to pull."
    [ "$DRY_RUN" = true ] && exit 0
    # Still allow --no-restart path to skip service restart gracefully
    NO_RESTART=true
    SKIP_REQUIREMENTS=true
else
    COMMITS_BEHIND=$(git rev-list --count HEAD.."origin/$CURRENT_BRANCH" 2>/dev/null || echo "?")
    info "Commits behind remote: $COMMITS_BEHIND"

    echo
    info "Pending changes:"
    git log HEAD.."origin/$CURRENT_BRANCH" --oneline --no-color 2>/dev/null \
        | while read -r line; do echo "      $line"; done

    if [ "$DRY_RUN" = true ]; then
        echo
        info "Dry-run complete. Run without --dry-run to apply the update."
        exit 0
    fi
    SKIP_REQUIREMENTS=false
fi

# ---------------------------------------------------------------------------
# 4. Stop services (unless --no-restart or already up-to-date)
# ---------------------------------------------------------------------------
if [ "$NO_RESTART" = false ]; then
    step "Stopping services"
    if [ -f "$STOP_SCRIPT" ]; then
        bash "$STOP_SCRIPT" && success "Services stopped." || warn "stop_services.sh returned non-zero (services may not have been running)."
    else
        warn "stop_services.sh not found — skipping service shutdown."
    fi
fi

# ---------------------------------------------------------------------------
# 5. Pull latest code
# ---------------------------------------------------------------------------
if [ "${SKIP_REQUIREMENTS:-false}" = false ]; then
    step "Pulling latest code"

    # Save the requirements hash before pulling to detect changes
    REQ_HASH_BEFORE=""
    if [ -f "$REQUIREMENTS_FILE" ]; then
        REQ_HASH_BEFORE=$(md5sum "$REQUIREMENTS_FILE" 2>/dev/null | awk '{print $1}' || sha256sum "$REQUIREMENTS_FILE" | awk '{print $1}')
    fi

    git pull origin "$CURRENT_BRANCH"
    NEW_SHA=$(git rev-parse --short HEAD)
    success "Updated to $NEW_SHA"

    # ---------------------------------------------------------------------------
    # 6. Reinstall requirements if requirements.txt changed
    # ---------------------------------------------------------------------------
    step "Checking requirements"

    REQ_HASH_AFTER=""
    if [ -f "$REQUIREMENTS_FILE" ]; then
        REQ_HASH_AFTER=$(md5sum "$REQUIREMENTS_FILE" 2>/dev/null | awk '{print $1}' || sha256sum "$REQUIREMENTS_FILE" | awk '{print $1}')
    fi

    if [ -z "$REQUIREMENTS_FILE" ] || [ ! -f "$REQUIREMENTS_FILE" ]; then
        warn "requirements.txt not found at $REQUIREMENTS_FILE — skipping pip install."
    elif [ "$REQ_HASH_BEFORE" = "$REQ_HASH_AFTER" ]; then
        info "requirements.txt unchanged — skipping pip install."
    else
        info "requirements.txt has changed — installing new dependencies..."
        if pip install -r "$REQUIREMENTS_FILE" --quiet; then
            success "Requirements updated successfully."
        else
            error "pip install failed. Check the output above."
            error "You can retry manually: pip install -r $REQUIREMENTS_FILE"
            exit 1
        fi
    fi

    # Make shell scripts executable in case new ones were added
    step "Refreshing script permissions"
    find "$SCRIPT_DIR" -name "*.sh" -exec chmod +x {} \;
    success "Shell scripts are executable."
fi

# ---------------------------------------------------------------------------
# 7. Restart services
# ---------------------------------------------------------------------------
if [ "$NO_RESTART" = false ]; then
    step "Restarting services"
    if [ -f "$START_SCRIPT" ]; then
        bash "$START_SCRIPT" && success "Services restarted." || {
            error "start_services.sh returned non-zero. Check the logs:"
            error "  tail -f $PROJECT_DIR/logs/gunicorn.log"
            exit 1
        }
    else
        warn "start_services.sh not found — skipping service restart."
    fi
fi

# ---------------------------------------------------------------------------
# 8. Summary
# ---------------------------------------------------------------------------
echo
bold "============================================================"
success "  ARNy_Plotter updated successfully!"
bold "============================================================"
info "From : $CURRENT_SHA"
info "To   : $(git rev-parse --short HEAD)"
info "Logs : $PROJECT_DIR/logs/"
[ "$NO_RESTART" = false ] && info "App  : http://localhost:4242"
bold "============================================================"
echo
