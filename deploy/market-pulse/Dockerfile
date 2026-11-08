# Market Pulse runtime image.  It intentionally installs only the packages the
# pulse service imports: the full terminal stack (pandas, TA-Lib, Rich) is not
# needed inside a collector/ingest container.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
RUN pip install --no-cache-dir "aiohttp>=3.12,<4" "asyncpg>=0.29,<1"

COPY hyperdata_terminal ./hyperdata_terminal

RUN useradd --create-home --uid 10001 pulse \
    && mkdir -p /raw/hyperliquid /raw/okx /raw/binance \
    && chown -R pulse:pulse /app /raw
USER pulse

ENTRYPOINT ["python", "-m", "hyperdata_terminal.market_pulse"]
CMD ["ingest"]
