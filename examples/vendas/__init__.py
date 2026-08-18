import regale

# VENDAS_DB_URL / DW_URL: names chosen here, not imposed by the framework —
# see compose.yaml (worker service) for where they're set in Docker, or
# export them yourself before running submit.py from the host.
regale.configure.add_db("vendas_db", regale.SQLSource(url=regale.env("VENDAS_DB_URL")))
regale.configure.add_db("dw", regale.SQLTarget(url=regale.env("DW_URL")))
