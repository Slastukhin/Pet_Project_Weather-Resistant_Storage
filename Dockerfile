FROM apache/airflow:3.3.1 AS builder

COPY requirements.txt /tmp/requirements.txt
RUN pip wheel --no-cache-dir --wheel-dir /tmp/wheels -r /tmp/requirements.txt

FROM apache/airflow:3.3.1

COPY --from=builder --chown=airflow:0 /tmp/wheels /tmp/wheels
RUN pip install --no-cache-dir /tmp/wheels/*.whl && rm -rf /tmp/wheels
