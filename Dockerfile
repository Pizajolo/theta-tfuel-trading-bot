FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot ./bot
COPY dashboard ./dashboard
COPY tools ./tools

RUN useradd --create-home --uid 1000 botuser && mkdir -p /app/data && chown -R botuser /app
USER botuser

# .env is mounted read-only at /app/.env (see docker-compose.yml) so it can be hot-reloaded.
CMD ["python", "-m", "bot", "--env", "/app/.env", "run"]
