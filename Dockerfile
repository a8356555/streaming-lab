# App image for event-gen / landing job / correctness tests.
# Pinned to 3.11 so pyiceberg/pyarrow wheels resolve (host may be newer).
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ librdkafka-dev \
    && apt-get clean \
    && find /var/lib/apt/lists -type f -delete

WORKDIR /app
COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt

COPY pyproject.toml ./
COPY src ./src
COPY scripts ./scripts
COPY tests ./tests
RUN pip install --no-cache-dir -e .

ENV PYTHONUNBUFFERED=1
CMD ["python", "-c", "print('streaming-lab app image')"]
