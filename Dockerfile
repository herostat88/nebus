FROM python:3.12-slim AS runtime
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.lock ./
RUN pip install -r requirements.lock
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-deps . && useradd --uid 10001 --create-home app
COPY alembic.ini ./
COPY migrations ./migrations
USER app
CMD ["uvicorn", "payments.api:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]

FROM runtime AS test
USER root
COPY requirements-test.lock ./
RUN pip install -r requirements-test.lock
COPY tests ./tests
USER app
CMD ["pytest", "-q"]
