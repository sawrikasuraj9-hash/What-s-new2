
FROM node:20-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    git \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies
COPY package*.json ./
RUN npm config set allow-git all && npm install

# Copy project source
COPY . .

# Build frontend
RUN npm run build

ENV NODE_ENV=production

EXPOSE 3000 7860 8080

# Give permissions to existing node user
RUN chown -R node:node /app

# Run as non-root user
USER node

# Start application
CMD ["npm", "start"]
