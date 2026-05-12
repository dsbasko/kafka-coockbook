# Lectures

All course material lives here — 9 modules, 42 units (38 lectures plus 4 use cases in `09-use-cases/`). The full course overview, table of contents, and instructions are in the [root README](../README.md). This file only describes what is inside this directory.

## Structure

```
lectures/
├── go.work                # workspace that ties all lecture modules together
├── go.work.sum
├── Makefile               # targets: list / lecture L=… / sync / build
├── internal/              # shared helpers (kafka, config, runctx, log)
├── 01-foundations/        # module 01 — Kafka foundations
├── 02-producer/           # module 02 — producer
├── 03-consumer/           # module 03 — consumer
├── 04-reliability/        # module 04 — reliability (EOS, outbox, retry/DLQ)
├── 05-contracts/          # module 05 — Protobuf, Schema Registry
├── 06-communication-patterns/  # module 06 — gRPC, sync vs async, saga
├── 07-streams-and-connect/     # module 07 — stream processing, Connect, Debezium
├── 08-operations/         # module 08 — monitoring, retention, sizing, troubleshooting
└── 09-use-cases/          # module 09 — cross-cutting use cases
```

Each lecture is a separate Go module with its own `go.mod`, a stub `README.md` linking to translations under `i18n/<lang>/README.md`, and a `Makefile` (at least a `run` target, plus something specific to the topic). Use cases are larger — several services, proto schemas, sometimes a `docker-compose.override.yml`.

Lecture layout after the i18n migration:

```
lectures/<NN-module>/<MM-slug>/
├── go.mod
├── README.md              # stub: links to i18n/ru and i18n/en
├── i18n/
│   ├── ru/README.md       # Russian original
│   └── en/README.md       # English translation (when ready)
├── images/                # shared by both languages
├── Makefile
└── cmd/<binary>/main.go
```

## Running

From the repo root:

```sh
make list                                                    # tree of all 42 lectures
make lecture L=01-foundations/01-01-architecture-and-kraft   # run a specific one
make sync                                                    # go work sync
make build                                                   # build every workspace module
```

`lectures/Makefile` owns these targets; the root `Makefile` proxies them.

## Conventions

- Module path: `github.com/dsbasko/kafka-sandbox/lectures/<NN-module>/<MM-short>`.
- Every lecture `go.mod` carries `replace github.com/dsbasko/kafka-sandbox/internal => ../../internal` — after the move into `lectures/` the relative path keeps the same `../../` depth.
- Inside a lecture: `cmd/<binary>/main.go` for one or two binaries, `pkg/` or flat files for anything bigger.
- READMEs are plain markdown (no MDX) and live under `i18n/<lang>/README.md`. Images stay in `images/` next to the lecture root; both translations reference them as `../../images/<file>.png`. Cross-module links use the form `[Title from course.yaml](../../../../<module>/<slug>/i18n/<lang>/README.md)` — from `i18n/<lang>/README.md` you climb four levels to `lectures/` and descend again. Siblings inside the same module use one less hop: `../../../<sibling-slug>/i18n/<lang>/README.md`. The trailing-slash form without `README.md` is also accepted (`../../../../<module>/<slug>/i18n/<lang>/`). The build fails if the target is missing from `course.yaml`, the path escapes `lectures/`, or the link points to a non-README markdown or a non-markdown resource. The site rewrites valid links to site URLs automatically.

## How to add a lecture

1. Create `lectures/<NN-module>/<MM-short-name>/` with `go.mod`, a stub `README.md`, `Makefile`, and `cmd/`.
2. Add `i18n/ru/README.md` (mandatory) and `i18n/en/README.md` (optional, can come later) with lecture content.
3. Add `use ./<NN-module>/<MM-short-name>` to `lectures/go.work`.
4. Describe the lecture in the root `course.yaml` (slug, bilingual title, duration, optional tags) — otherwise the site build fails in `generateStaticParams`.
5. Update the Table of Contents in the root `README.md` (use `make web-generate-readme-toc` to regenerate it).
6. Run `make web-check-coverage` — it should report `mismatches: 0`.
