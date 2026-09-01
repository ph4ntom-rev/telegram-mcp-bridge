FROM python:3.12-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /build

COPY requirements.lock pyproject.toml README.md LICENSE ./
COPY src ./src

RUN python -m pip install --prefix=/install --require-hashes -r requirements.lock \
    && python -m pip install --prefix=/install --no-deps .

FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH=/usr/local/bin:$PATH

RUN groupadd --system bridge \
    && useradd --system --gid bridge --home-dir /nonexistent --shell /usr/sbin/nologin bridge \
    && install -d -o bridge -g bridge -m 0700 /data

COPY --from=builder /install /usr/local

USER bridge:bridge
WORKDIR /data
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/readyz', timeout=2).read()"]

ENTRYPOINT ["telegram-mcp"]
CMD ["serve", "--transport", "http", "--env-file", "/run/secrets/bridge.env"]
