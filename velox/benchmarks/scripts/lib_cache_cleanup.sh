#!/usr/bin/env bash
# Sourced by the Task 018-D benchmark orchestration scripts. Provides
# sentinel-based, fail-close cache cleanup so a mis-set path can never wipe an
# arbitrary directory.
#
# Safety model (mirrors the C++ helper dwio::common::bench::clearBenchmarkCacheRoot
# in velox/dwio/common/benchmarks/CacheReadHarness.cpp):
#   * Every destructive call resolves DIR and ROOT with `realpath -m` and refuses
#     to act unless DIR is a *strict component child* of ROOT (a proper
#     descendant, decided component-by-component so a sibling like `<root>2`
#     that merely shares a string prefix is rejected).
#   * A directory is only wiped when it carries an authentic sentinel: a regular,
#     non-symlink file named exactly `.velox_benchmark_cache_sentinel`. The
#     benchmark binaries preserve this file when they reset their cache roots, so
#     the EXIT trap here can still authenticate and remove the run directory.
#   * The filesystem root, the current working directory and DIR==ROOT are always
#     refused.
set -euo pipefail

# Exact sentinel name shared with the C++ contract (kCacheSentinelName in
# velox/dwio/common/benchmarks/CacheReadHarness.h). Do not rename without
# updating that header and Task 018-D.
SENTINEL_NAME=".velox_benchmark_cache_sentinel"

# Registry of (dir, root) pairs to clean on exit; appended by setup_trap_cleanup.
# Declared before any function so the trap handlers never need a top-level
# `local` (which is a syntax error outside a function body).
_CLEANUP_DIRS=()
_CLEANUP_ROOTS=()
_CLEANUP_TRAP_ARMED=0

# True when `child` is a strict (proper) component-wise descendant of `base`.
# Both arguments must already be absolute, lexically-normal paths (as produced by
# `realpath -m`). Equal paths return false. The check appends a trailing slash to
# `base` and strips it as a *literal* prefix, so the match can only succeed on a
# component boundary -- `/a/tmp2/x` is NOT considered a child of `/a/tmp`.
_is_strict_component_child() {
  local child="$1"
  local base="$2"
  local base_slash rest
  [[ "$child" != "$base" ]] || return 1
  if [[ "$base" == "/" ]]; then
    base_slash="/"
  else
    base_slash="$base/"
  fi
  rest="${child#"$base_slash"}"
  # Prefix absent -> not nested; prefix present but nothing after -> equal.
  [[ "$rest" != "$child" ]] || return 1
  [[ -n "$rest" ]] || return 1
  return 0
}

# Validates that DIR may be safely operated on relative to ROOT. Rejects empty
# inputs, non-absolute resolutions, the filesystem root, the current working
# directory, DIR==ROOT, and any DIR that is not a strict component child of ROOT.
validate_cache_dir() {
  local dir="${1-}"
  local root="${2-}"
  if [[ -z "$dir" || -z "$root" ]]; then
    echo "ERROR: cache dir and root must be non-empty (dir='$dir' root='$root')" >&2
    return 1
  fi
  local real_dir real_root cwd
  real_dir="$(realpath -m -- "$dir")"
  real_root="$(realpath -m -- "$root")"
  if [[ "$real_dir" != /* || "$real_root" != /* ]]; then
    echo "ERROR: resolved paths must be absolute (dir='$real_dir' root='$real_root')" >&2
    return 1
  fi
  if [[ "$real_dir" == "/" || "$real_root" == "/" ]]; then
    echo "ERROR: refusing to operate on the filesystem root (dir='$real_dir' root='$real_root')" >&2
    return 1
  fi
  cwd="$(realpath -m -- "$PWD")"
  if [[ "$real_dir" == "$cwd" ]]; then
    echo "ERROR: refusing to operate on the current working directory: $real_dir" >&2
    return 1
  fi
  if [[ "$real_dir" == "$real_root" ]]; then
    echo "ERROR: cache dir must not equal its root: $real_dir" >&2
    return 1
  fi
  if ! _is_strict_component_child "$real_dir" "$real_root"; then
    echo "ERROR: cache dir '$real_dir' is not a strict child of root '$real_root'" >&2
    return 1
  fi
  return 0
}

# create_sentinel DIR ROOT
# Validates (DIR strict child of ROOT), then creates DIR and writes a fresh
# sentinel as a mode-0600 regular file. Refuses to reuse any pre-existing marker
# -- regular, non-regular, or symlink (including a dangling one) -- and creates
# the file without following a symlink via `noclobber` (set -C) plus a 0177 umask.
create_sentinel() {
  local dir="${1-}"
  local root="${2-}"
  validate_cache_dir "$dir" "$root" || return 1
  local real_dir marker payload
  real_dir="$(realpath -m -- "$dir")"
  marker="$real_dir/$SENTINEL_NAME"
  # -e is false for a dangling symlink, so test -L explicitly too: any existing
  # marker of any type is refused rather than silently reused/overwritten.
  if [[ -e "$marker" || -L "$marker" ]]; then
    echo "ERROR: refusing to reuse a pre-existing sentinel marker: $marker" >&2
    return 1
  fi
  mkdir -p -- "$real_dir"
  payload="velox-benchmark-$$-$(date +%s)"
  # Subshell isolates `set -C` and the umask. noclobber makes `>` fail if the
  # target exists (so we never write through a symlink or clobber a file);
  # umask 0177 yields a 0600 regular file. $$ is the parent shell PID even here.
  if ! (
    set -C
    umask 0177
    printf '%s\n' "$payload" > "$marker"
  ); then
    echo "ERROR: failed to create sentinel (does it already exist?): $marker" >&2
    return 1
  fi
  if [[ -L "$marker" || ! -f "$marker" ]]; then
    echo "ERROR: sentinel is not a regular file after creation: $marker" >&2
    return 1
  fi
  return 0
}

# safe_wipe_cache_dir DIR ROOT
# Validates, requires an authentic (regular, non-symlink) sentinel inside DIR,
# then removes only the resolved DIR and verifies it is gone. No unresolved
# variables and no globbing: the target is the quoted, realpath-resolved child.
safe_wipe_cache_dir() {
  local dir="${1-}"
  local root="${2-}"
  validate_cache_dir "$dir" "$root" || return 1
  local real_dir marker
  real_dir="$(realpath -m -- "$dir")"
  marker="$real_dir/$SENTINEL_NAME"
  # -f follows symlinks (true only for a regular file); ! -L rejects a symlink
  # named like the sentinel. Together: a genuine regular file is required.
  if [[ -L "$marker" || ! -f "$marker" ]]; then
    echo "ERROR: refusing to wipe '$real_dir': missing authentic sentinel '$SENTINEL_NAME'" >&2
    return 1
  fi
  rm -rf -- "$real_dir"
  if [[ -e "$real_dir" || -L "$real_dir" ]]; then
    echo "ERROR: failed to remove cache dir: $real_dir" >&2
    return 1
  fi
  return 0
}

# Wipes every registered (dir, root) pair through safe_wipe_cache_dir. Runs from
# the EXIT/signal traps, so all `local`s stay inside this function body and the
# trap strings themselves contain no `local`. Returns nonzero if any wipe failed.
_run_cleanup() {
  local rc=0
  local n="${#_CLEANUP_DIRS[@]}"
  local i=0
  while [ "$i" -lt "$n" ]; do
    if ! safe_wipe_cache_dir "${_CLEANUP_DIRS[$i]}" "${_CLEANUP_ROOTS[$i]}"; then
      rc=1
    fi
    i=$((i + 1))
  done
  return "$rc"
}

# EXIT trap body. Preserves an original nonzero exit status; if the script
# succeeded (status 0) but cleanup failed, exits nonzero instead. `_exit_status`
# is captured first, as a plain (non-local) assignment, so `$?` is not disturbed.
_on_exit() {
  _exit_status=$?
  _cleanup_status=0
  _run_cleanup || _cleanup_status=$?
  if [ "$_exit_status" -ne 0 ]; then
    exit "$_exit_status"
  fi
  exit "$_cleanup_status"
}

# INT/TERM trap body. Detaches the EXIT trap (so cleanup is not run twice), wipes
# the registered dirs, and exits 130 (128 + signal) regardless of cleanup result.
_on_signal() {
  trap - EXIT
  _run_cleanup || true
  exit 130
}

# setup_trap_cleanup DIR ROOT
# Registers a (dir, root) pair for trap-driven cleanup and arms the EXIT/INT/TERM
# traps exactly once. Duplicate (dir, root) registrations are ignored so a pair
# is never wiped (or reported) twice.
setup_trap_cleanup() {
  local dir="${1-}"
  local root="${2-}"
  local n="${#_CLEANUP_DIRS[@]}"
  local i=0
  while [ "$i" -lt "$n" ]; do
    if [[ "${_CLEANUP_DIRS[$i]}" == "$dir" && "${_CLEANUP_ROOTS[$i]}" == "$root" ]]; then
      return 0
    fi
    i=$((i + 1))
  done
  _CLEANUP_DIRS+=("$dir")
  _CLEANUP_ROOTS+=("$root")
  if [ "$_CLEANUP_TRAP_ARMED" -eq 0 ]; then
    trap '_on_exit' EXIT
    trap '_on_signal' INT TERM
    _CLEANUP_TRAP_ARMED=1
  fi
  return 0
}

# validate_benchmark_binary BIN
# Requires BIN to be executable and to resolve (symlinks followed) to a path that
# names a RelWithDebInfo or Release build directory. Debug build binaries are
# refused, per the Task 018 global constraint forbidding Debug benchmarks.
validate_benchmark_binary() {
  local bin="${1-}"
  if [[ -z "$bin" ]]; then
    echo "ERROR: validate_benchmark_binary: empty binary path" >&2
    return 1
  fi
  if [[ ! -x "$bin" ]]; then
    echo "ERROR: benchmark binary is not executable: $bin" >&2
    return 1
  fi
  local real lower probe
  real="$(realpath -- "$bin")"
  lower="${real,,}"
  # Strip the allowed 'relwithdebinfo' token before the Debug scan so it is not a
  # false positive (it does not contain the substring "debug", but stripping is
  # explicit and future-proof), then reject any genuine Debug build path.
  probe="${lower//relwithdebinfo/}"
  case "$probe" in
    *debug*)
      echo "ERROR: refusing Debug benchmark binary (RelWithDebInfo/Release required): $real" >&2
      return 1
      ;;
  esac
  case "$lower" in
    *relwithdebinfo* | *release*)
      return 0
      ;;
    *)
      echo "ERROR: benchmark binary is not from a RelWithDebInfo/Release build dir: $real" >&2
      return 1
      ;;
  esac
}
