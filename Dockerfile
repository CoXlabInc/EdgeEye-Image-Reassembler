FROM aler9/rtsp-simple-server:latest AS rtsp
FROM node:18-alpine

RUN apk add --no-cache ffmpeg

WORKDIR /root/

COPY ./mjpeg-streamer .
RUN npm install --only=production

COPY --from=rtsp /mediamtx .
COPY --from=rtsp /mediamtx.yml .

CMD [ "/root/mediamtx" ]
