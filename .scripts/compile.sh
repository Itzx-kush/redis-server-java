#!/bin/sh

set -e

mvn -q -B package -Ddir=/tmp/my-redis-build
