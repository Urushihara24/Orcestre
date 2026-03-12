FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (minimal). curl for healthchecks/debug.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Runtime sources only.
COPY main.py /app/main.py
COPY recheck_manager.py /app/recheck_manager.py
COPY campaign_settings.py /app/campaign_settings.py
COPY db_models.py /app/db_models.py
COPY epic_api_client.py /app/epic_api_client.py
COPY epic_device_auth.py /app/epic_device_auth.py
COPY tools /app/tools

# Default: all-in-one (bot + scheduler). Override with APP_MODE.
CMD ["python", "main.py"]
