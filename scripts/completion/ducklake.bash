_ducklake_complete() {
  local cur
  cur="${COMP_WORDS[COMP_CWORD]}"
  local cmds="doctor quickstart up down status seed query smoke validate browser logs prod-up prod-down completion help"
  COMPREPLY=( $(compgen -W "${cmds}" -- "${cur}") )
}

complete -F _ducklake_complete ./scripts/ducklake
complete -F _ducklake_complete ducklake
