#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

env_file="${1:-.env.production}"
if [ ! -f "$env_file" ]; then
  echo "missing env file: $env_file"
  echo "create it from .env.production.example"
  exit 1
fi

# shellcheck disable=SC1090
set -a; source "$env_file"; set +a

err=0

require_nonempty() {
  local name="$1"
  local value="${!name:-}"
  if [ -z "$value" ]; then
    echo "ERROR: $name is required"
    err=1
  fi
}

reject_placeholder() {
  local name="$1"
  local value="${!name:-}"
  if echo "$value" | grep -Eiq 'change-me|replace-with|your-tailnet'; then
    echo "ERROR: $name contains placeholder value"
    err=1
  fi
}

min_len() {
  local name="$1"
  local n="$2"
  local value="${!name:-}"
  if [ -n "$value" ] && [ "${#value}" -lt "$n" ]; then
    echo "ERROR: $name should be at least $n characters"
    err=1
  fi
}

require_nonempty CORS_ALLOWED_ORIGINS
require_nonempty RUNNER_AUTH_TOKENS
reject_placeholder CORS_ALLOWED_ORIGINS
reject_placeholder RUNNER_AUTH_TOKENS

if [ -z "${API_KEYS:-}" ] && [ -z "${JWT_HS256_SECRET:-}" ]; then
  echo "ERROR: set either API_KEYS or JWT_HS256_SECRET"
  err=1
fi

if [ -n "${API_KEYS:-}" ]; then
  reject_placeholder API_KEYS
  min_len API_KEYS 20
fi
if [ -n "${JWT_HS256_SECRET:-}" ]; then
  reject_placeholder JWT_HS256_SECRET
  min_len JWT_HS256_SECRET 32
fi

IFS=',' read -r -a runner_tokens <<< "${RUNNER_AUTH_TOKENS:-}"
if [ "${#runner_tokens[@]}" -lt 1 ]; then
  echo "ERROR: RUNNER_AUTH_TOKENS must include at least one token"
  err=1
fi
for t in "${runner_tokens[@]}"; do
  tt="$(echo "$t" | xargs)"
  if [ "${#tt}" -lt 20 ]; then
    echo "ERROR: each RUNNER_AUTH_TOKENS token should be at least 20 characters"
    err=1
    break
  fi
done

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is required"
  err=1
fi

if [ "$err" -ne 0 ]; then
  exit 1
fi

docker compose --env-file "$env_file" -f docker-compose.yml -f docker-compose.production.yml config >/dev/null
echo "production preflight passed for $env_file"
