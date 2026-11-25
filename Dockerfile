
FROM node:20-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    git \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies
COPY package*.json ./
RUN npm install

# Copy project source
COPY . .

# Build frontend
RUN npm run build

ENV NODE_ENV=production

EXPOSE 3000 7860 8080

# Set permissions
RUN chown -R node:node /app

USER node

# Start server
CMD ["npm", "start"]
