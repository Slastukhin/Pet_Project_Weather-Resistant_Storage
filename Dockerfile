FROM apache/airflow:3.3.1 AS builder

COPY requirements.txt /tmp/requirements.txt
RUN pip wheel --no-cache-dir --wheel-dir /tmp/wheels -r /tmp/requirements.txt

FROM apache/airflow:3.3.1

COPY --from=builder --chown=airflow:0 /tmp/wheels /tmp/wheels
RUN pip install --no-cache-dir /tmp/wheels/*.whl && rm -rf /tmp/wheels

COPY dbt-requirements.txt /tmp/dbt-requirements.txt
RUN python -m venv /opt/airflow/dbt-venv \
    && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/dbt-requirements.txt
