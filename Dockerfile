FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    ENV=production \
    LOG_FILE=/var/log/edge-tts/service.log

WORKDIR /app

RUN addgroup --system app && adduser --system --ingroup app app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy standalone application code
COPY app/ ./app/
COPY main.py ./main.py
COPY entrypoint.sh /entrypoint.sh

RUN mkdir -p /var/log/edge-tts /data/edge_tts/segments \
    && sed -i 's/\r$//' /entrypoint.sh \
    && chown -R app:app /var/log/edge-tts /app /data/edge_tts \
    && chmod +x /entrypoint.sh

EXPOSE 8100

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8100/health', timeout=3).read()"

ENTRYPOINT ["/entrypoint.sh"]
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8100", "--workers", "1", "--timeout-keep-alive", "180"]
