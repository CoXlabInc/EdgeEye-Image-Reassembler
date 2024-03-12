FROM aler9/rtsp-simple-server:latest AS rtsp
FROM node:18-alpine

RUN apk add --no-cache ffmpeg font-terminus font-inconsolata font-dejavu font-noto font-noto-cjk font-awesome font-noto-extra

WORKDIR /root/

COPY --from=rtsp /mediamtx .
COPY --from=rtsp /mediamtx.yml .

COPY ./mjpeg-streamer .
RUN npm install --only=production
CMD [ "/root/mediamtx" ]
