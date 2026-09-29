FROM apache/airflow:3.3.1 AS builder

COPY requirements.txt /tmp/requirements.txt
RUN pip wheel --no-cache-dir --wheel-dir /tmp/wheels -r /tmp/requirements.txt

FROM apache/airflow:3.3.1

COPY --from=builder --chown=airflow:0 /tmp/wheels /tmp/wheels
RUN pip install --no-cache-dir /tmp/wheels/*.whl && rm -rf /tmp/wheels

COPY dbt-requirements.txt /tmp/dbt-requirements.txt
RUN python -m venv /opt/airflow/dbt-venv \
    && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/dbt-requirements.txt

COPY --chown=airflow:0 dbt/my_dwh/packages.yml dbt/my_dwh/package-lock.yml /opt/airflow/dbt-cache/
RUN printf 'name: dependency_cache\nversion: "1.0"\nconfig-version: 2\n' > /opt/airflow/dbt-cache/dbt_project.yml \
    && /opt/airflow/dbt-venv/bin/dbt deps --project-dir /opt/airflow/dbt-cache
