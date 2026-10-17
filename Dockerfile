FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY engine/ engine/
RUN go mod download && CGO_ENABLED=0 go build -o /kavach ./engine

FROM python:3.11-slim
RUN pip install --no-cache-dir flask gunicorn requests psycopg2-binary
WORKDIR /app
COPY --from=build /kavach /usr/local/bin/kavach
COPY app.py ./
COPY sdk/python ./sdk/python
COPY static ./static
COPY start.sh ./
RUN chmod +x start.sh
ENV PYTHONPATH=/app/sdk/python \
    KAVACH_URL=http://127.0.0.1:8080 \
    KAVACH_KEY=appkey \
    KAVACH_SECRET=appsecret
CMD ["./start.sh"]
