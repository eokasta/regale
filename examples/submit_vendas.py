"""Publishes the "vendas" pipeline's partitions to the Redis stream the
Compose worker(s) consume from.

Deliberately NOT inside examples/vendas/ — discover("examples.vendas")
walks that whole package tree via pkgutil, and importing a sibling module
that calls submit() as a side effect of merely discovering the pipeline
would be a bug, not a feature.

Run from the repository root, with the infra up (`docker compose up -d
redis origem destino`):

    uv run python -m examples.submit_vendas

Importing examples.vendas configures its source/target connections eagerly
(see examples/vendas/__init__.py), which requires VENDAS_DB_URL and DW_URL
to be set even here on the host — submit() itself never queries either
database, but the module-level regale.configure.add_db(...) calls that
happen on import do need the env vars to exist. Point them at the
host-exposed ports:

    export VENDAS_DB_URL=postgresql+psycopg://regale:regale@localhost:5433/vendas
    export DW_URL=postgresql+psycopg://regale:regale@localhost:5434/dw
"""

import os

import regale

regale.discover("examples.vendas")

broker_url = os.environ.get("REGALE_BROKER", "redis://localhost:6379/0")
count = regale.submit("vendas", broker=broker_url, stream="vendas-tasks")
print(f"published {count} partition(s) to stream 'vendas-tasks'")
