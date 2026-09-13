# meshcore-tcp-mux

## Development environment

### Direnv

This project uses direnv to load `.env` and `.envrc`. Run commands that
need the project environment as:

    direnv exec . <command>

Prefer Makefile targets where available:

    direnv exec . make
    direnv exec . make spec
    direnv exec . make clean

For direct Crystal commands, also use `direnv exec .` so that
`CRYSTAL_CACHE_DIR` stays inside this repository:

    direnv exec . crystal <arguments>

Do not override `CRYSTAL_CACHE_DIR` or place Crystal cache files outside
the project. If `.envrc` is blocked, ask the user to run `direnv allow`.

### Asdf

This project uses `asdf` to manage its version of `crystal`, configured in `.tool-versions`.
