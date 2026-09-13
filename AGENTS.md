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

## Crystal style

- Use `class`, not `module`, including for namespaces and utility containers.
  Use explicit class methods for shared helpers rather than module mixins.
- Use `MeshCoreTCPMux` for the project namespace and `src/meshcore_tcp_mux/`
  for its implementation files.
- Keep `src/main.cr` a thin wrapper that requires the entrypoint file and calls
  `MeshCoreTCPMux::BinaryEntrypoint.new`. CLI setup belongs in
  `src/meshcore_tcp_mux/binary_entrypoint.cr`.

## Comments and spec readability

- Put class and method documentation immediately **inside** the declaration,
  not above it. Put spec overviews inside the relevant helper class or
  `describe` block, not at the top of the file.
- Explain what code/tests do and why, especially protocol state transitions,
  ownership, ordering, and failure handling. Do not assume the reader already
  knows the wire protocol or the test harness.
- Comment all hard-coded byte-array fixtures, including expected responses and
  malformed inputs. Identify command versus response, opcode names, important
  fields/offsets, lengths, byte order, and the purpose of synthetic values.
  Explain why malformed fixtures are invalid and why boundary values matter.
- Walk through complex specs as conversations: who sends each command, which
  client owns the operation, which responses are visible or hidden, and what
  each assertion proves. Distinguish command acceptance from radio delivery.
- Keep comments accurate when changing code; preserve explanatory detail rather
  than replacing it with opaque constants or unexplained helper calls.

## Portable, non-identifying examples

- Do not hard-code personal contact/channel names, private deployment addresses,
  real keys, or user-specific filesystem paths in shared code, specs, or docs.
- Use clearly synthetic fixtures and configurable destinations. Prefer portable
  interpreter shebangs and keep local/live-test identifying details out of commits.
