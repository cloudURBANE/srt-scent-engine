# syntax=docker/dockerfile:1
# AWS (App Runner / ECS Fargate) image — replaces nixpacks.toml for the AWS
# migration (docs/AWS_MIGRATION_PLAN.md). Railway keeps building with nixpacks
# because railway.toml pins `builder = "nixpacks"`, so this file is inert there.
#
# Debian bookworm's `chromium` apt package is a real binary (not the Ubuntu
# snap-transition shim that broke DrissionPage on stock Railway images), and
# the `xvfb` package ships `xvfb-run`, which the headful clearance-mint path
# resolves via shutil.which().
FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
      chromium xvfb gcc ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
# Runtime deps only — requirements-dev.txt is for the test suite.
RUN pip install --no-cache-dir -r requirements.txt

# fg_cache/ JSON caches ship in the image deliberately: App Runner has no
# persistent disk, and the durable detail cache is the DB (fg_detail_cache).
COPY . .

# DRISSION_FORCE_ORIGIN=1 is load-bearing off Railway: the DevTools-websocket
# Origin patch in fragrance_parser_full_rewrite_fixed.py otherwise only
# activates when RAILWAY_* env vars are present, and without it every
# Chromium clearance mint fails with a DevTools websocket 404.
ENV BASENOTES_CHROMIUM_HEADLESS=1 \
    DISABLE_CHROMIUM_MINT=1 \
    DRISSION_FORCE_ORIGIN=1 \
    BASENOTES_CHROMIUM_PATH=/usr/bin/chromium \
    FRAGRANTICA_CHROMIUM_PATH=/usr/bin/chromium \
    PORT=8000

# Surfaces the build in /api/diagnostics/runtime (replaces RAILWAY_GIT_COMMIT_SHA).
ARG GIT_COMMIT=""
ENV SOURCE_VERSION=${GIT_COMMIT} \
    GIT_COMMIT=${GIT_COMMIT}

EXPOSE 8000

CMD ["sh", "-c", "uvicorn api:app --host 0.0.0.0 --port ${PORT}"]
