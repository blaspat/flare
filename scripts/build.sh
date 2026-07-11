#!/usr/bin/env bash
# Flare — cross-platform build script
# Usage: ./scripts/build.sh [version]
# If version is omitted, it reads the latest git tag or uses "dev".
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# --- Version ---------------------------------------------------------------
VERSION="${1:-}"
if [ -z "$VERSION" ]; then
    VERSION="$(git describe --tags --always --dirty 2>/dev/null || echo "dev")"
fi

LDFLAGS="-w -s -X main.version=${VERSION}"

# --- Output directory ------------------------------------------------------
OUTDIR="${ROOT}/dist/flare-${VERSION}"
rm -rf "$OUTDIR"
mkdir -p "$OUTDIR"

# --- Build matrix -----------------------------------------------------------
PLATFORMS=(
    "linux/amd64"
    "linux/arm64"
    "windows/amd64"
    "windows/arm64"
)

echo "Building Flare ${VERSION}"
echo ""

for PLATFORM in "${PLATFORMS[@]}"; do
    GOOS="${PLATFORM%%/*}"
    GOARCH="${PLATFORM##*/}"
    BINARY="flare-${GOOS}-${GOARCH}"
    if [ "$GOOS" = "windows" ]; then
        BINARY="${BINARY}.exe"
    fi

    echo "  → ${GOOS}/${GOARCH} ..."

    GOOS="$GOOS" GOARCH="$GOARCH" CGO_ENABLED=0 \
        go build -trimpath -ldflags "${LDFLAGS}" -o "${OUTDIR}/${BINARY}" .

    # Compress with UPX if available
    if command -v upx &>/dev/null; then
        upx --quiet --best "${OUTDIR}/${BINARY}" 2>/dev/null || true
    fi
done

# --- Checksums --------------------------------------------------------------
echo ""
echo "Generating checksums..."
cd "$OUTDIR"
sha256sum flare-* > checksums.txt
cd "$ROOT"

# --- Copy extras (optional) -------------------------------------------------
if [ -f README.md ]; then cp README.md "$OUTDIR/"; fi
[ -f config.example.toml ] && cp config.example.toml "$OUTDIR/"

echo ""
echo "Build complete: ${OUTDIR}"
ls -lh "$OUTDIR" | tail -n +2
