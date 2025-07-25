# Apache Sedona on Railway

This project deploys Apache Sedona using Railway.

## Deployment

1. Connect this repository to Railway
2. Railway will automatically detect the Dockerfile and deploy
3. The service will be available on the provided Railway URL

## Configuration

- Base image: `apache/sedona:latest`
- Default port: 8080 (configurable)
- Health check enabled with 300s timeout
