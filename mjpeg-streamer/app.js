// -*- mode: js; js-indent-level: 4; indent-tabs-mode:nil; -*-

import os from 'os';
import http from 'http';
import sharp from 'sharp';

import { Command } from 'commander';
const program = new Command();

import redis from 'redis';
import pjson from './package.json' with { type: 'json' }

program
    .version(pjson.version)
    .description(pjson.description)
    .option('-r --redis <URL>', 'Redis URL (default redis://localhost)')
    .option('-p --port <n>', 'port number (default 8080)', parseInt)
    .option('-v --version', 'show version')
    .parse(process.argv);

const redisUrl = program.opts().redis || process.env.REDIS_URL || 'redis://localhost';
const port = program.opts().port || 8080;
const boundaryID = "boundary_id";

// Primary Redis client for data fetching
var redisClient = redis.createClient({
    url: redisUrl
});

try {
    await redisClient.connect();
    console.log('Redis connected');
} catch(error) {
    console.error('Redis connect fail');
    console.error(error);
    process.exit(1);
}

async function fetchAndSendImage(res, bufferKey, timestampKey, locale, timezone, height, mjpeg) {
    if (res.writableEnded) return;

    let buffer = await redisClient.GET(redis.commandOptions({ returnBuffers: true }), bufferKey);
    
    if (!buffer || buffer.length === 0) {
        // Fallback to last known good image
        buffer = await redisClient.GET(redis.commandOptions({ returnBuffers: true }), bufferKey + ':last');
    }

    if (buffer && buffer.length > 0) {
        try {
            let image = sharp(buffer, { failOn: 'none' });
            const metadata = await image.metadata();
            
            let rawTimestamp = await redisClient.GET(timestampKey);
            let timestampStr = "Unknown";
            if (rawTimestamp) {
                timestampStr = new Date(rawTimestamp).toLocaleString(locale, { timeZone: timezone });
            }

            const processedBuffer = await image.composite([{
                input: {
                    text: {
                        text: timestampStr,
                        width: metadata.width,
                        height: height,
                        align: "left",
                    }
                },
                top: 0,
                left: 0
            }]).jpeg().toBuffer();

            if (mjpeg) {
                res.write('Content-Type: image/jpeg\r\n');
                res.write(`Content-Length: ${processedBuffer.length}\r\n\r\n`);
                res.write(processedBuffer, 'binary');
                res.write('\r\n--' + boundaryID + '\r\n');
                console.log(`Sent frame: ${processedBuffer.length} bytes`);
            } else {
                res.writeHead(200, { 'Content-Type': 'image/jpeg' });
                res.end(processedBuffer, 'binary');
            }
        } catch (e) {
            console.error(`Image processing error: ${e.message}`);
        }
    }
}

/**
 * create a server to serve out the motion jpeg images
 */
var server = http.createServer(async (req, res) => {
    let uri = req.url.split('?');
    let path = uri[0].split('/').slice(1);
    let params = new URLSearchParams(uri[1]);
    
    if (path.length >= 1 && path[0] !== 'healthcheck' && path[0] !== '') {
        const device = path[0];
        const isLastRequest = path.length === 2 && path[1] === 'last';
        
        let bufferKey = `ImageToRtsp:${device}:image`;
        let timestampKey = `ImageToRtsp:${device}:sense_time`;

        if (isLastRequest) {
            bufferKey += ':last';
            timestampKey += ':last';
        }

        let mjpeg = params.get('mjpeg') !== 'false';
        let locale = params.get('locale') || Intl.DateTimeFormat().resolvedOptions().locale;
        let timezone = params.get('timezone') || Intl.DateTimeFormat().resolvedOptions().timeZone;
        let height = parseInt(params.get('height')) || 30;

        if (mjpeg) {
            res.writeHead(200, {
                'Content-Type': 'multipart/x-mixed-replace;boundary="' + boundaryID + '"',
                'Connection': 'keep-alive',
                'Cache-Control': 'no-cache, no-store, max-age=0, must-revalidate',
                'Pragma': 'no-cache'
            });
            res.write('--' + boundaryID + '\r\n');

            // Send initial frame
            await fetchAndSendImage(res, bufferKey, timestampKey, locale, timezone, height, true);

            // Setup Pub/Sub for live updates
            const subscriber = redisClient.duplicate();
            await subscriber.connect();
            
            const updateChannel = `EdgeEye:updated:${device}`;
            let heartbeatTimer = null;

            const sendHeartbeat = async () => {
                if (res.writableEnded) return;
                console.log(`Heartbeat for ${device}`);
                await fetchAndSendImage(res, bufferKey, timestampKey, locale, timezone, height, true);
                resetHeartbeat();
            };

            const resetHeartbeat = () => {
                if (heartbeatTimer) clearTimeout(heartbeatTimer);
                heartbeatTimer = setTimeout(sendHeartbeat, 10000); // 10s heartbeat
            };

            await subscriber.subscribe(updateChannel, async (message) => {
                console.log(`Update notification received for ${device}`);
                await fetchAndSendImage(res, bufferKey, timestampKey, locale, timezone, height, true);
                resetHeartbeat();
            });

            resetHeartbeat();

            res.on('close', async () => {
                console.log(`Client disconnected for ${device}`);
                if (heartbeatTimer) clearTimeout(heartbeatTimer);
                await subscriber.unsubscribe(updateChannel);
                await subscriber.disconnect();
            });

        } else {
            // Single image request
            await fetchAndSendImage(res, bufferKey, timestampKey, locale, timezone, height, false);
        }
    } else if (req.url === "/healthcheck") {
        res.statusCode = 200;
        res.end();
    } else {
        res.statusCode = 404;
        res.end('Not Found');
    }
});

server.on('error', function(e) {
    console.error(`Server error: ${e.message}`);
    process.exit(1);
});

server.listen(port, () => {
    console.log(`${pjson.name} started on port ${port}`);
});
