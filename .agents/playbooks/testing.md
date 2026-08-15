# Playbook — testing a change

## Before touching anything

```bash
task test
```

Record the number it prints. A suite that was already red stays attributable to
whoever made it red; skipping this is how a pre-existing failure becomes "the change
broke it".

At 2026-08-15 the baseline was **74 passed in 0.30s**.

## The layers

| Layer | Command | Proves |
|---|---|---|
| unit | `task test` | the governance logic is correct. Fast, no services, no docker |
| thin core | `task test:thin` | `celine.governance` still installs and passes on core dependencies alone — the boundary that keeps it shareable |
| import sweep | see below | every submodule imports on the target Python. A version-gated import can sit in a module no test touches |
| integration | `cd integration-tests && task start && task run` | the pipeline runner drives real Meltano, dbt, Postgres and Marquez |

### unit

```bash
task test                    # -> uv run pytest tests/
task test -- -k governance   # CLI_ARGS pass through
```

### thin core

```bash
task test:thin
```

Rebuilds `.venv-thin` from scratch, installs the package **with no extras**, and runs
the whole suite against it. Observed 2026-08-15: 17 packages installed, `74 passed`.

That the *entire* `tests/` suite passes on a core install is the useful fact — every
test in `tests/` is a governance test, so the unit layer and this layer differ only
in what is installed underneath them.

This mirrors `.github/workflows/governance-thin.yaml`, which additionally asserts
that no heavy package (sqlalchemy, prefect, meltano, dbt, openlineage, keycloak,
pandas) reached the install. **Run it whenever a change touches
`src/celine/governance/`, `schema/`, or `pyproject.toml` dependencies.** A new import
in a governance module is one line, works in a developer environment where every
extra is installed, and fails at a consumer's install.

Note the CI job uses `.venv/bin/python` rather than `uv run` on purpose: `uv run`
syncs from `uv.lock` including the dev group, which silently replaces the no-extras
install being tested. Do not "simplify" it.

### import sweep

CI walks every submodule of `celine.governance` and `celine.utils` and imports it,
because a version-gated import can live in a module no test exercises. To reproduce
locally:

```bash
uv run python - <<'EOF'
import importlib, pkgutil, sys
failed, total = [], 0
for root in ("celine.governance", "celine.utils"):
    pkg = importlib.import_module(root)
    names = [root] + [m.name for m in pkgutil.walk_packages(pkg.__path__, root + ".")]
    total += len(names)
    for name in names:
        try:
            importlib.import_module(name)
        except Exception as exc:
            failed.append(f"{name}: {type(exc).__name__}: {exc}")
print(f"imported {total - len(failed)}/{total} submodules")
for f in failed:
    print("FAIL", f)
sys.exit(1 if failed else 0)
EOF
```

This needs `--all-extras` installed to be meaningful.

### integration

```bash
cd integration-tests
task start          # docker compose up -d  — Postgres, Marquez, …
task run            # uv run pytest -vv
task stop
```

From the repository root, `task integration-tests -- <args>` runs the suite **without
starting the stack**. Start it first.

`task reset` (`docker compose down -v`) destroys the volumes, discarding Marquez
lineage history and the seeded database. Use it to clear inconsistent state, never
before investigating a failure.

## The CI matrix is generated, not written

`.github/workflows/test.yaml` builds its Python matrix from the
`Programming Language :: Python :: 3.x` classifiers in `pyproject.toml`. Adding or
removing a supported version means editing the classifiers — there is no second list,
and changing one without the other cannot happen.

CI installs `--all-extras` deliberately: the extras are where a version floor would
actually appear. The floor this package used to declare came in through an
extras-only dependency, so testing the core alone would leave the claim unproven
exactly where it failed before.

## Declaring what a test verifies

Requirements live in `docs/specifications/index.md` and carry `REQ-####`
identifiers. A test declares what it verifies with a tag on its own line directly
above it:

```python
# @verifies REQ-0002
def test_offered_but_unlisted_is_reported():
    ...
```

`.agents/harness.toml` declares nothing, so the defaults apply: provider `harness`,
pattern `REQ-[0-9]{4}`. The trace matrix is the projection of the register and the
tags — generated, never hand-written.

Rules:

- **Never tag with an identifier that is not in the register.** A tag naming a
  requirement nobody wrote measures nothing and reads as coverage.
- **Adding a requirement is a conversation, not a decision to take mid-change.** The
  scope is currently the shared surface only — the five properties other repositories
  depend on. Pipeline execution and admin are deliberately out.
- A change that alters behaviour a requirement covers changes the requirement in the
  same commit, or it is not the same requirement any more.

Scope and the reasoning behind it: `.agents/plans/requirements-baseline.md`.

## Reporting

Name the layers that ran, the layers that did not, and why. A layer skipped because
it needs docker you did not start is a fact about the evidence, not an admission.

A green run is only evidence about what actually ran. `task test` passing says
nothing about whether the thin boundary still holds, and `task test:thin` passing
says nothing about the pipeline runner — the integration layer is the only thing that
exercises `celine.utils.pipelines` at all.
