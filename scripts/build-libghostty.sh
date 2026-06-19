#!/usr/bin/env bash
# Build the static libghostty-vt that the go-libghostty cgo grid links, for one
# target, into .libghostty/ghostty-<NAME>/. Prints that target's PKG_CONFIG_PATH
# (the dir holding libghostty-vt-static.pc) on stdout's last line.
#
# Usage:  scripts/build-libghostty.sh <NAME> <ZIG_TARGET>
#   e.g.  scripts/build-libghostty.sh macos-arm64   aarch64-macos
#         scripts/build-libghostty.sh linux-amd64   x86_64-linux-musl
#
# Needs: go, cmake, and Zig 0.15.x on PATH (or $ZIG). The pure-Go x/vt grid
# (CGO_ENABLED=0) needs none of this — this is only for the go-libghostty default
# (CI release binaries) and for verifying the cgo grid locally.
#
# macOS note: mise's upstream Zig 0.15.2 cannot link the Xcode 26 SDK
# (ziglang/zig#31658). Locally use Homebrew's patched zig:
#   ZIG=/opt/homebrew/opt/zig@0.15/bin/zig scripts/build-libghostty.sh ...
# CI installs a working Zig directly.
set -euo pipefail

NAME="${1:?usage: build-libghostty.sh <NAME> <ZIG_TARGET>}"
ZIG_TARGET="${2:?usage: build-libghostty.sh <NAME> <ZIG_TARGET>}"
ZIG="${ZIG:-zig}"

BUILD_DIR=".libghostty"
GHOSTTY_SRC="$BUILD_DIR/_deps/ghostty-src"
PREFIX="$PWD/$BUILD_DIR/ghostty-$NAME"

# Fetch the pinned ghostty source once via go-libghostty's CMake FetchContent (the
# configure step clones it; we drive the actual lib build with zig directly below so
# we get both libghostty-vt.a and libghostty-vt-static.pc, which only the shared
# emit-lib-vt path produces).
if [ ! -d "$GHOSTTY_SRC" ]; then
	MOD="$(go list -m -f '{{.Dir}}' go.mitchellh.com/libghostty)"
	cmake -S "$MOD" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release -DZIG_EXECUTABLE="$ZIG" >&2
fi

( cd "$GHOSTTY_SRC" && "$ZIG" build \
	-Demit-lib-vt=true -Demit-xcframework=false \
	-Dtarget="$ZIG_TARGET" -Doptimize=ReleaseFast \
	--prefix "$PREFIX" >&2 )

test -f "$PREFIX/lib/libghostty-vt.a" || { echo "missing libghostty-vt.a for $NAME" >&2; exit 1; }
test -f "$PREFIX/share/pkgconfig/libghostty-vt-static.pc" || { echo "missing libghostty-vt-static.pc for $NAME" >&2; exit 1; }
echo "$PREFIX/share/pkgconfig"
