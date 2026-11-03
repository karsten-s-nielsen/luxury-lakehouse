# lakehouse v2

lakehouse v2 ingests open soccer data from five providers into a local or Hugging Face lake. It keeps every artifact
that a source delivers, links the same players, teams, competitions and matches across providers, and builds gold
dimension tables with dbt. The design is in `docs/specs/2026-10-26-v2-ingestion-design.md`.

## Architecture

Open [`architecture.html`](architecture.html) in a browser to explore the C4 diagrams (System Context, Containers,
Components for the Ops Space and the Job, the schedule/run dynamic flow, and the Hugging Face and local deployments).
The source is [`architecture.dsl`](architecture.dsl) (Structurizr DSL).

## Install

You need [uv](https://docs.astral.sh/uv/). uv installs Python 3.12 for the project when it is not on the machine.

Windows (PowerShell):

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
cd v2
uv sync --locked
uv run lakehouse --help
```

macOS and Linux:

```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
cd v2
uv sync --locked
uv run lakehouse --help
```

`uv sync --locked` installs the exact versions in `uv.lock`. All the examples below use `uv run lakehouse`.

## Create a lake

```sh
uv run lakehouse init --lake "D:/Soccer/lake"
```

`init` does three things:

1. It creates the lake folder and the restricted folder. The restricted folder is beside the lake folder, with
   `-restricted` added to its name (`D:/Soccer/lake-restricted`). Use `--restricted <path>` to put it somewhere else.
2. It writes the bootstrap file, which names the two folders for the commands that follow.
3. It saves the laptop defaults as config version 1. Use `--profile staging` for a staging machine. If the lake
   already has a config, `init` keeps it.

`init` creates local folders only. The owner creates the Hugging Face buckets of an HF lake.

### Where the lake lives

The lake folder holds the published data (`data/`), the catalog (`catalog/`), the config (`config/`) and the run
records (`runs/`). The restricted folder holds the files of private matches. Keep the restricted folder private:
never share it, sync it to a public place or put it in a repository.

Every command resolves the two roots as a pair, from the first source that names a lake root:

1. the options `--lake <root>` and `--restricted <root>`;
2. the environment variables `LAKEHOUSE_LAKE_ROOT` and `LAKEHOUSE_RESTRICTED_ROOT`;
3. the bootstrap file `bootstrap.json`:
   - Windows: `%APPDATA%\lakehouse\`;
   - macOS: `~/Library/Application Support/lakehouse/`;
   - Linux: `$XDG_CONFIG_HOME/lakehouse/` (default `~/.config/lakehouse/`);
   - or the folder that `LAKEHOUSE_CONFIG_DIR` names.

The restricted root comes only from the source that named the lake root. A root can also be an HF bucket,
`hf://buckets/<namespace>/<name>`. An HF lake must name both buckets.

If a lake folder is missing (for example, a disconnected drive), every command stops with an error and creates
nothing. If you move a lake, run `relocate` (see below).

## Create the operator account

The local web app signs you in with one operator account:

```sh
uv run lakehouse account create ops
```

The command asks for the password twice. The password needs at least 12 characters. The lake stores only an Argon2id
hash of it. Use `--password-stdin` to read the password from the first line of standard input, and `--replace` to
replace an existing account. An HF lake has no local account: the HF Space signs in with Hugging Face.

## The default modules

A module is one provider through one channel. The laptop defaults keep the first download small and still show
cross-provider linking:

| Module | Source | Licence | Default |
|---|---|---|---|
| `statsbomb_public` | GitHub `statsbomb/open-data` (with 360 data) | `NOASSERTION`: the repository has `LICENSE.pdf` | active: 1. Bundesliga 2023/24, FIFA World Cup 2018, FIFA World Cup 2022 |
| `wyscout_public` | figshare collection 4415000 | CC BY 4.0 | active: FIFA World Cup 2018 |
| `idsse_public` | HF dataset `pysport/idsse-data` | CC-BY-4.0 | active: all 7 matches |
| `skillcorner_public` | GitHub `SkillCorner/opendata` | MIT | active: all matches |
| `gradientsports_public` | a public Google Drive folder | no written licence | inactive: a large download |
| `idsse_pining`, `statsbomb_pining`, `skillcorner_pining`, `gradientsports_pining` | the pining API | the licence of each entry (`source.licence`) | inactive |

Read the licence of each source before you use or share its data. The Gradient Sports folder has no written licence.
The StatsBomb licence is in the repository's `LICENSE.pdf`.

The pining modules read `PINING_FOR_THE_DATA_TOKEN` (the owner token), else `PINING_LIST_TOKEN` (the list-only
token), from the environment. Without one, they use the documented public token. Set a token only in the environment,
never in the config. Set `GITHUB_TOKEN` to raise the GitHub rate limit for the GitHub sources.

## Change the config

```sh
uv run lakehouse config show
uv run lakehouse config set raw_archive true
uv run lakehouse config set modules.gradientsports_public.active true
uv run lakehouse config set --defaults laptop
```

`config set` takes a dotted key and a value. A module setting is `modules.<module id>.<field>`. The value is JSON
(`true`, `3`, `"text"`, a list) or plain text. Each save is a new config version and records the operator. Use the
global option `--operator <name>` to record a name other than your login name.

## Run the ingestion

```sh
uv run lakehouse run
```

One run lists every active module, downloads what changed, parses it into bronze tables, links the entities, builds
the dbt models, publishes a new catalog version and cleans up. A second run downloads only what changed. The run
writes JSON-line logs and one JSON result line to standard output.

Exit codes of every command:

| Code | Meaning |
|---|---|
| 0 | Done. |
| 1 | Failed: for example a failed match, a `verify` problem or a guard stop. The result line names the errors. |
| 2 | Usage, config or gating error: for example a missing root, an invalid value, or a command that HF allows only in an HF Job. |
| 3 | Skipped: another run holds the lease. Try again after that run. |

## Schedule the runs

The schedule is in the config. Set the cron expression first, then switch the schedule on, then install the OS task:

```sh
uv run lakehouse config set schedule.cron "0 6 * * *"
uv run lakehouse config set schedule.enabled true
uv run lakehouse schedule install
uv run lakehouse schedule status
uv run lakehouse schedule uninstall
```

`schedule install` registers a native task: Task Scheduler on Windows, launchd on macOS, a systemd timer on Linux, or
cron where systemd is missing. A missed run is not replayed, and at most one run starts when the machine wakes. The
task gets the roots of this lake and these variables from the environment that installs it: `LAKEHOUSE_*`,
`UV_CACHE_DIR`, `UV_PYTHON_INSTALL_DIR`, `HF_HOME` and `TMPDIR`. A task never holds a secret.

On an always-on machine, use a system-level task. It runs as the operator account, never as root or SYSTEM:

```sh
uv run lakehouse schedule install --system --run-as ops
```

On a Hugging Face lake there is no OS task: the schedule is a Hugging Face scheduled Job that the owner creates once,
and the Ops Space Settings page changes its cron, pauses, resumes and triggers it. The HF deploy, the scheduled job
and the token setup are in [`docs/ops/deploy.md`](docs/ops/deploy.md); failures are in
[`docs/ops/troubleshooting.md`](docs/ops/troubleshooting.md).

## Other commands

| Command | What it does |
|---|---|
| `lakehouse verify` | Checks every kept catalog version in full: the catalog file's size and SHA-256, and every file it names. |
| `lakehouse restore --to <version>` | Points the lake at a copy of a kept catalog version. The next run continues from it. |
| `lakehouse relocate --from <old root> --to <new root>` | After you move a lake, rewrites the catalog's file paths in a new version, and updates the bootstrap file. |
| `lakehouse reparse --all` | Parses again from the raw archive, offline, every match whose parser version changed. |
| `lakehouse reparse --match <provider>:<match id>` | Parses one match again from the raw archive. |
| `lakehouse sweep --allow-large` | Repeats a cleanup that stopped because it would delete more than 10 % of the files. Check the list first. |
| `lakehouse decisions export <file>` | Writes the review decisions (must-link and cannot-link) to a JSON file. |
| `lakehouse decisions import <file>` | Adds the decisions of an exported file. |

`reparse` needs the raw archive (`config set raw_archive true` before the runs that download the data).

On an HF lake, `restore`, `relocate` and `sweep --allow-large` run only inside an HF Job that the owner launches with
the gateway key. Without the key, they stop before they write anything.
