# VIXION

Narrative engine — base técnica PRIME (PostgreSQL + pgvector + Alembic).

## Base de datos

```bash
export DATABASE_URL="postgresql+psycopg://USER:PASS@HOST:5432/DBNAME"
alembic upgrade head
```

Requisitos: PostgreSQL con extensión host compatible (la migración ejecuta `CREATE EXTENSION vector`).

## Tests de integración

```bash
export DATABASE_URL="postgresql+psycopg://USER:PASS@HOST:5432/DBNAME"
pip install -e ".[dev]"
pytest tests/integration -q
```

Sin `DATABASE_URL`, los tests `@pytest.mark.integration` se omiten.

## Worker (cola Postgres)

```bash
export DATABASE_URL=postgresql+psycopg://...
vixion-job-runner
# o bucle: vixion-job-runner --loop
```

## Desarrollo

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```
