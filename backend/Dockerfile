# Stage 1: build the React dashboard
FROM node:22-alpine AS ui
WORKDIR /ui
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund --fetch-retries=6 --fetch-retry-maxtimeout=120000
COPY frontend/ ./
RUN npm run build

# Stage 2: FastAPI serving the API and the built dashboard
FROM python:3.11-slim
WORKDIR /srv
COPY backend/requirements.txt /srv/backend/requirements.txt
RUN pip install --no-cache-dir --retries 10 --timeout 120 -r /srv/backend/requirements.txt
COPY common /srv/common
COPY backend /srv/backend
COPY --from=ui /ui/dist /srv/frontend/dist
WORKDIR /srv/backend
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
