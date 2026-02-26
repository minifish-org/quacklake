#compdef ducklake ./scripts/ducklake

_ducklake() {
  local -a cmds
  cmds=(
    'doctor:check local dependencies'
    'quickstart:guided setup'
    'up:start services'
    'down:stop services'
    'status:show compose status'
    'seed:seed demo parquet data'
    'query:run sample API query'
    'smoke:run smoke test'
    'validate:run full validation matrix'
    'browser:show browser URL'
    'logs:tail service logs'
    'prod-up:start production profile'
    'prod-down:stop production profile'
    'completion:print shell completion'
    'help:show usage'
  )
  _describe 'ducklake command' cmds
}

compdef _ducklake ducklake
compdef _ducklake ./scripts/ducklake
