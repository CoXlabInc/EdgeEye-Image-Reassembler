FROM node:22-alpine

# Install fonts for timestamp overlay
RUN apk add --no-cache \
    font-terminus font-inconsolata font-dejavu font-noto \
    font-noto-cjk font-awesome font-noto-extra

WORKDIR /root/

# Install Node.js dependencies
COPY ./mjpeg-streamer/package*.json ./
RUN npm install --only=production

# Copy MJPEG streamer source
COPY ./mjpeg-streamer .

# Start MJPEG streamer
CMD [ "npm", "start", "--", "-r", "redis://redis" ]
