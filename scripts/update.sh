#!/bin/bash
# =============================================================================
# update.sh — Update ARNy_Plotter to the latest version from GitHub
#
# The installer copies files without .git, so this script clones a fresh
# copy to a temp directory and syncs it over the installation directory —
# the same approach used by install_script.py.
#
# Usage:
#   ./scripts/update.sh              # update and restart services
#   ./scripts/update.sh --no-restart # update code only, keep services running
#   ./scripts/update.sh --dry-run    # show what would change, do nothing
#   ./scripts/update.sh --branch dev # pull a specific branch
#   ./scripts/update.sh --help
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
TEMP_DIR="$(mktemp -d /tmp/arny_update_XXXXXX)"
CLONE_DIR="$TEMP_DIR/ARNy_Plotter"

# Directories/files never overwritten during update (local runtime data)
PRESERVE=(
    "logs"
    "static/uploads"
    ".env"
)

# Directories excluded from sync (same as installer)
EXCLUDE_FROM_SYNC=(
    ".git"
    "__pycache__"
    ".github"
    "*.pyc"
)

# ---------------------------------------------------------------------------
# Colour helpers
# ---------------------------------------------------------------------------
_tty() { [ -t 1 ] && echo true || echo false; }
USE_COLOR=$(_tty)

color() {
    local code="$1"; shift
    if [ "$USE_COLOR" = "true" ]; then
        printf "\033[%sm%s\033[0m\n" "$code" "$*"
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
# Cleanup on exit
# ---------------------------------------------------------------------------
cleanup() {
    if [ -d "$TEMP_DIR" ]; then
        rm -rf "$TEMP_DIR"
    fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
NO_RESTART=false
DRY_RUN=false
BRANCH="main"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-restart) NO_RESTART=true; shift ;;
        --dry-run)    DRY_RUN=true;    shift ;;
        --branch)
            shift
            BRANCH="${1:-main}"
            shift
            ;;
        -h|--help)
            echo "Usage: ./scripts/update.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --no-restart     Sync code but do not restart services"
            echo "  --dry-run        Show pending changes without applying them"
            echo "  --branch NAME    Pull a specific branch (default: main)"
            echo "  -h, --help       Show this help message"
            exit 0
            ;;
        *)
            error "Unknown option: $1  (use --help for usage)"
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
info "Branch     : $BRANCH"
info "Project    : $PROJECT_DIR"
[ "$DRY_RUN"    = true ] && warn "Mode       : DRY RUN — no changes will be applied"
[ "$NO_RESTART" = true ] && info "Mode       : --no-restart — services will NOT be restarted"
bold "============================================================"

# ---------------------------------------------------------------------------
# 1. Verify prerequisites
# ---------------------------------------------------------------------------
step "Checking prerequisites"

if ! command -v git &>/dev/null; then
    error "git not found in PATH. Please install git and try again."
    exit 1
fi
success "git: $(git --version)"

if ! command -v pip &>/dev/null && ! command -v pip3 &>/dev/null; then
    warn "pip not found — requirement updates will be skipped."
fi

# ---------------------------------------------------------------------------
# 2. Clone the repository into a temp directory
# ---------------------------------------------------------------------------
step "Fetching latest code from GitHub"
info "Cloning $REPO_URL (branch: $BRANCH) ..."

if [ "$DRY_RUN" = false ]; then
    git clone --depth 1 --branch "$BRANCH" "$REPO_URL" "$CLONE_DIR" --quiet \
        || { error "git clone failed. Check your internet connection and try again."; exit 1; }
    success "Clone complete."
else
    # In dry-run mode, use git ls-remote to check the remote SHA
    REMOTE_SHA=$(git ls-remote "$REPO_URL" "refs/heads/$BRANCH" 2>/dev/null | awk '{print substr($1,1,7)}' || echo "unknown")
    info "Remote SHA ($BRANCH): $REMOTE_SHA"
fi

# ---------------------------------------------------------------------------
# 3. Show what will change (diff between install dir and new clone)
# ---------------------------------------------------------------------------
step "Comparing versions"

if [ "$DRY_RUN" = false ]; then
    REMOTE_SHA=$(git -C "$CLONE_DIR" rev-parse --short HEAD)
    info "New version SHA: $REMOTE_SHA"

    # Show files that differ (informational)
    CHANGED_COUNT=0
    while IFS= read -r -d '' new_file; do
        rel="${new_file#$CLONE_DIR/}"
        existing="$PROJECT_DIR/$rel"
        if [ -f "$existing" ]; then
            if ! diff -q "$new_file" "$existing" &>/dev/null; then
                CHANGED_COUNT=$((CHANGED_COUNT + 1))
            fi
        else
            CHANGED_COUNT=$((CHANGED_COUNT + 1))
        fi
    done < <(find "$CLONE_DIR" -type f \
        -not -path "*/.git/*" \
        -not -path "*/__pycache__/*" \
        -not -name "*.pyc" \
        -print0)

    if [ "$CHANGED_COUNT" -eq 0 ]; then
        success "No file changes detected — already up to date."
        NO_RESTART=true
    else
        info "$CHANGED_COUNT file(s) will be updated."
    fi
else
    # Dry-run: just report the remote state
    info "Remote HEAD: $REMOTE_SHA"
    echo
    info "To see detailed changes visit:"
    info "  $REPO_URL/commits/$BRANCH"
    echo
    info "Dry-run complete. Run without --dry-run to apply the update."
    exit 0
fi

# ---------------------------------------------------------------------------
# 4. Hash requirements.txt before syncing (to detect changes)
# ---------------------------------------------------------------------------
REQ_HASH_BEFORE=""
if [ -f "$REQUIREMENTS_FILE" ]; then
    REQ_HASH_BEFORE=$(md5sum "$REQUIREMENTS_FILE" 2>/dev/null | awk '{print $1}' \
                   || sha256sum "$REQUIREMENTS_FILE" | awk '{print $1}')
fi

# ---------------------------------------------------------------------------
# 5. Stop services
# ---------------------------------------------------------------------------
if [ "$NO_RESTART" = false ]; then
    step "Stopping services"
    if [ -f "$STOP_SCRIPT" ]; then
        bash "$STOP_SCRIPT" \
            && success "Services stopped." \
            || warn "stop_services.sh returned non-zero (services may not have been running)."
    else
        warn "stop_services.sh not found — skipping service shutdown."
    fi
fi

# ---------------------------------------------------------------------------
# 6. Sync files from clone to installation directory
# ---------------------------------------------------------------------------
step "Syncing files"

# Build rsync exclude args
RSYNC_EXCLUDES=()
for excl in "${EXCLUDE_FROM_SYNC[@]}"; do
    RSYNC_EXCLUDES+=(--exclude="$excl")
done
for pres in "${PRESERVE[@]}"; do
    RSYNC_EXCLUDES+=(--exclude="$pres")
done

if command -v rsync &>/dev/null; then
    # rsync is available — clean, fast, shows a diff
    rsync -a --delete \
        "${RSYNC_EXCLUDES[@]}" \
        "$CLONE_DIR/" "$PROJECT_DIR/" \
        --out-format="      updated: %n"
    success "Sync complete (rsync)."
else
    # Fallback: plain copy using cp (same logic as install_script.py)
    warn "rsync not found — falling back to cp."
    for item in "$CLONE_DIR"/*/  "$CLONE_DIR"/.[^.]* "$CLONE_DIR"/*; do
        [ -e "$item" ] || continue
        name="$(basename "$item")"

        # Skip excluded items
        skip=false
        for excl in "${EXCLUDE_FROM_SYNC[@]}"; do
            [[ "$name" == $excl ]] && skip=true && break
        done
        # Skip preserved items
        for pres in "${PRESERVE[@]}"; do
            base_pres="$(basename "$pres")"
            [[ "$name" == "$base_pres" ]] && skip=true && break
        done
        $skip && continue

        dest="$PROJECT_DIR/$name"
        if [ -d "$item" ]; then
            rm -rf "$dest"
            cp -r "$item" "$dest"
        else
            cp -f "$item" "$dest"
        fi
        echo "      updated: $name"
    done
    success "Sync complete (cp fallback)."
fi

# ---------------------------------------------------------------------------
# 7. Reinstall pip requirements if requirements.txt changed
# ---------------------------------------------------------------------------
step "Checking requirements"

REQUIREMENTS_FILE="$PROJECT_DIR/scripts/installation/requirements.txt"
REQ_HASH_AFTER=""
if [ -f "$REQUIREMENTS_FILE" ]; then
    REQ_HASH_AFTER=$(md5sum "$REQUIREMENTS_FILE" 2>/dev/null | awk '{print $1}' \
                  || sha256sum "$REQUIREMENTS_FILE" | awk '{print $1}')
fi

PIP_CMD=""
command -v pip  &>/dev/null && PIP_CMD="pip"
command -v pip3 &>/dev/null && PIP_CMD="pip3"

if [ ! -f "$REQUIREMENTS_FILE" ]; then
    warn "requirements.txt not found at $REQUIREMENTS_FILE — skipping pip install."
elif [ -z "$PIP_CMD" ]; then
    warn "pip not available — skipping requirement installation."
elif [ "$REQ_HASH_BEFORE" = "$REQ_HASH_AFTER" ]; then
    info "requirements.txt unchanged — skipping pip install."
else
    info "requirements.txt changed — installing new dependencies..."
    if $PIP_CMD install -r "$REQUIREMENTS_FILE" --quiet; then
        success "Requirements updated."
    else
        error "pip install failed."
        error "Retry manually: $PIP_CMD install -r $REQUIREMENTS_FILE"
        exit 1
    fi
fi

# ---------------------------------------------------------------------------
# 8. Refresh script permissions
# ---------------------------------------------------------------------------
step "Refreshing script permissions"
find "$SCRIPT_DIR" -name "*.sh" -exec chmod +x {} \;
success "Shell scripts are executable."

# ---------------------------------------------------------------------------
# 9. Restart services
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
# 10. Summary
# ---------------------------------------------------------------------------
echo
bold "============================================================"
success "  ARNy_Plotter updated successfully!"
bold "============================================================"
info "Version : $REMOTE_SHA"
info "Logs    : $PROJECT_DIR/logs/"
[ "$NO_RESTART" = false ] && info "App     : http://localhost:4242"
bold "============================================================"
echo
