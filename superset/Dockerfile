FROM apache/superset:4.0.1
USER root
# clickhouse-sqlalchemy>=0.3.0 pulls SQLAlchemy>=2.0 which is incompatible with alembic in superset:4.0.1
# 0.2.9 is the last version compatible with SQLAlchemy 1.x (which superset bakes in)
RUN pip install --no-cache-dir clickhouse-sqlalchemy==0.2.9
USER superset
