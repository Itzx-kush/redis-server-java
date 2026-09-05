#!/bin/sh

set -e

exec java --enable-preview \
  -jar /tmp/my-redis-build/codecrafters-redis.jar "$@"
