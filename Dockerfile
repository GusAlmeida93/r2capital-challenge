FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# The 2.10+ airflow image runs pip inside an active virtualenv at
# /home/airflow/.local, so plain `pip install` puts packages there
# (already on PATH). We use uv for the actual install: pip's resolver
# blows past its depth limit on the airflow + cosmos + dbt graph, and
# Airflow's constraints file pins versions (e.g. duckdb==1.1.3,
# protobuf==4.25.5) that conflict with the bumps we need for the UI
# extension and dbt 1.9. uv's resolver handles this in seconds.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir uv \
    && uv pip install --no-cache -r /requirements.txt
