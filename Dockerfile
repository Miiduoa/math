# syntax=docker/dockerfile:1
FROM node:18-alpine

# Create app directory
WORKDIR /app

# Copy entire repo (frontend + backend)
COPY . /app

# Install only backend deps
WORKDIR /app/backend
RUN npm install --only=production || true

ENV PORT=8787
EXPOSE 8787

CMD ["node","server.js"]
