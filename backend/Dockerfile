# syntax=docker/dockerfile:1
FROM node:18-alpine
WORKDIR /app
# Copy backend
COPY backend/package.json ./
RUN npm install --only=production || true
COPY backend/server.js ./server.js
# Copy frontend static files to parent context path resolution
COPY index.html /app/..
COPY styles.css /app/..
COPY app.js /app/..
COPY db.js /app/..
ENV PORT=8787
EXPOSE 8787
CMD ["node","server.js"]
