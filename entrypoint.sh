#!/bin/sh
set -e

mkdir -p /data/edge_tts/segments /var/log/edge-tts
chown -R app:app /data/edge_tts /var/log/edge-tts

# Dynamically locate and load jemalloc if installed to optimize memory arenas
JEMALLOC_PATH=$(find /usr/lib -name "libjemalloc.so.2" -print -quit 2>/dev/null)
if [ -n "$JEMALLOC_PATH" ]; then
    export LD_PRELOAD="$JEMALLOC_PATH"
fi

exec "$@"
