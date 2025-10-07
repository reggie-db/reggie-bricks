#!/usr/bin/env bash
set -euo pipefail
exec caddy run --config '/Users/reggie.pierce/Projects/reggie-bricks-py/.apps/caddy/Caddyfile' --adapter caddyfile
