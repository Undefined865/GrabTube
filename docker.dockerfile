FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

ENV GRABTUBE_DIR=/data
VOLUME ["/data"]
EXPOSE 8000

RUN useradd -m -u 1000 grabtube \
 && mkdir -p /data \
 && chown -R grabtube:grabtube /data /app
USER grabtube

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request,sys; \
      sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/api/health',timeout=4).status==200 else 1)"

CMD ["python", "app.py", "--host", "0.0.0.0", "--port", "8000"]