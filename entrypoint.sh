#!/bin/sh
set -e

mkdir -p /data/edge_tts/segments /var/log/edge-tts
chown -R app:app /data/edge_tts /var/log/edge-tts

exec "$@"
