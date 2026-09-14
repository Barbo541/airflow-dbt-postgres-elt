# Public portfolio publication

## Why a sanitized public history is recommended

The current tree contains original synthetic fixtures and retained MIT attribution.
Older private Git objects may still contain inherited datasets or media whose
separate redistribution rights were not established. The upstream software
license does not automatically license unrelated third-party assets.

For a public portfolio, the safest path is therefore to publish the **current
sanitized tree with a new public history**, while keeping `LICENSE`, `NOTICE.md`,
and the upstream reference intact.

## Recommended release path

Do not run these steps inside the working/private repository. Export the reviewed
tree into a separate directory, verify it, then initialize the public repository
there.

Conceptually:

```text
private engineering history
        ↓ current reviewed tree only
sanitized release directory
        ↓ new Git history
public portfolio repository
```

Before the first public push, verify:

- `.env` and local credentials are absent;
- only synthetic `raw_data/*.csv` fixtures are present;
- no screenshots/media from the inherited history are included;
- `LICENSE` and `NOTICE.md` are present;
- no machine-specific paths or personal secrets are present;
- Ruff, pytest, and `python scripts/validate.py` pass;
- README commands match the release tree;
- the Git author email used for the public commit is the address you intend to expose.

The first public commit should describe this as a derived/refactored portfolio
release, not a from-scratch implementation. Do not remove the upstream copyright
notice or provenance simply to make the project appear exclusive.

## Suggested GitHub presentation

Repository description:

> Production-minded local ELT pipeline demonstrating transactional ingestion, Airflow orchestration, dbt contracts/tests, clean-room CI, and PostgreSQL performance validation.

Suggested topics:

`data-engineering`, `airflow`, `dbt`, `postgresql`, `docker`, `python`, `etl`,
`elt`, `data-quality`, `github-actions`, `portfolio`

Create the `v1.0.0` tag only after the clean-room validation has passed on the
final public tree.
