// -*- mode: js; js-indent-level: 4; indent-tabs-mode:nil; -*-

import os from 'os';
import http from 'http';
import fs from 'fs/promises';
import path from 'path';
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
    .option('-u --upload-url <URL>', 'Upload URL for composed images')
    .option('-H --upload-headers <JSON>', 'JSON string of HTTP headers for upload')
    .option('-M --det-upload-mode <n>', 'Det upload mode (1=with snap, 2=det first+with snap, 3=det first+snap alone)', parseInt)
    .option('-O --upload-overlay <items>', 'Upload snap overlay (comma-separated: timestamp,bbox,none)')
    .option('-s --save-dir <path>', 'Save directory for completed images (default empty = disabled)')
    .option('-v --version', 'show version')
    .parse(process.argv);

const redisUrl = program.opts().redis || process.env.REDIS_URL || 'redis://localhost';
const port = program.opts().port || 8080;
const uploadUrl = program.opts().uploadUrl || process.env.UPLOAD_URL || '';
const uploadHeaders = program.opts().uploadHeaders || process.env.UPLOAD_HEADERS || '';
const detUploadMode = program.opts().detUploadMode || parseInt(process.env.DET_UPLOAD_MODE) || 2;
const uploadOverlayStr = program.opts().uploadOverlay || process.env.UPLOAD_OVERLAY || 'timestamp,bbox';
const overlayParts = uploadOverlayStr.toLowerCase().split(',').map(s => s.trim());
const uploadOverlay = {
    timestamp: overlayParts.includes('timestamp'),
    bbox: overlayParts.includes('bbox') || overlayParts.includes('det'),
};
const saveDir = program.opts().saveDir || process.env.SAVE_DIR || '';
const boundaryID = "boundary_id";
const HEX16_RE = /^[a-f0-9]{16}$/;

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

// Global subscriber for det uploads (independent of MJPEG connections)
const globalSub = redisClient.duplicate();
await globalSub.connect();
await globalSub.pSubscribe('EdgeEye:updated:*', async (message, channel) => {
    const device = channel.replace('EdgeEye:updated:', '');
    if (message === "det") {
        doUploadDet(device);
    } else {
        doUpload(device);
    }
});

function buildComposites(metadata, timestampStr, detections, overlay = { timestamp: true, bbox: true }) {
    const composites = [];
    if (overlay.timestamp) {
        const fontSize = Math.max(10, Math.floor(metadata.height * 0.09));
        const timestampSvg = `<svg width="${metadata.width}" height="${fontSize + 8}" xmlns="http://www.w3.org/2000/svg">
<text x="2" y="${fontSize}" fill="white" font-size="${fontSize}" font-family="monospace" stroke="black" stroke-width="0.5">${timestampStr}</text>
</svg>`;
        composites.push({
            input: Buffer.from(timestampSvg),
            top: 0,
            left: 0
        });
    }
    if (overlay.bbox && detections && detections.length > 0) {
        const colors = ["#00ff00", "#ff0000", "#00ffff", "#ffff00", "#ff00ff", "#0000ff", "#ffffff"];
        let svgParts = [`<svg width="${metadata.width}" height="${metadata.height}" xmlns="http://www.w3.org/2000/svg">`];
        for (let k = 0; k < detections.length; k++) {
            const d = detections[k];
            const px = d.x * metadata.width;
            const py = d.y * metadata.height;
            const pw = d.w * metadata.width;
            const ph = d.h * metadata.height;
            const color = colors[k % colors.length];
            const scorePct = Math.round(d.score * 100);
            svgParts.push(`<rect x="${px - pw/2}" y="${py - ph/2}" width="${pw}" height="${ph}" stroke="${color}" stroke-width="2" fill="none"/>`);
            svgParts.push(`<text x="${px - pw/2 + 2}" y="${py - ph/2 - 4}" fill="${color}" font-size="13" font-family="monospace">${d.class}(${scorePct}%)</text>`);
        }
        svgParts.push('</svg>');
        composites.push({
            input: Buffer.from(svgParts.join('')),
            top: 0,
            left: 0
        });
    }
    return composites;
}

async function fetchAndSendImage(res, bufferKey, timestampKey, mjpeg, detKey) {
    if (res.writableEnded) return;

    let buffer = await redisClient.GET(redis.commandOptions({ returnBuffers: true }), bufferKey);
    
    if (!buffer || buffer.length === 0) {
        buffer = await redisClient.GET(redis.commandOptions({ returnBuffers: true }), bufferKey + ':last');
    }

    if (buffer && buffer.length > 0) {
        try {
            let image = sharp(buffer, { failOn: 'none' });
            const metadata = await image.metadata();
            
            let rawTimestamp = await redisClient.GET(timestampKey);
            let timestampStr = "Unknown";
            if (rawTimestamp) {
                const date = new Date(rawTimestamp);
                timestampStr = date.toLocaleString('sv-SE', { timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone });
            }

            let detections = null;
            if (detKey) {
                let rawDet = await redisClient.GET(detKey);
                if (rawDet) detections = JSON.parse(rawDet);
            }

            const composites = buildComposites(metadata, timestampStr, detections);
            const processedBuffer = await image.composite(composites).jpeg().toBuffer();

            if (mjpeg) {
                res.write('Content-Type: image/jpeg\r\n');
                res.write(`Content-Length: ${processedBuffer.length}\r\n\r\n`);
                res.write(processedBuffer, 'binary');
                res.write('\r\n--' + boundaryID + '\r\n');
                return processedBuffer.length;
            } else {
                res.writeHead(200, { 'Content-Type': 'image/jpeg' });
                res.end(processedBuffer, 'binary');
                return processedBuffer.length;
            }
        } catch (e) {
            console.error(`Image processing error: ${e.message}`);
        }
    }
    return 0;
}

async function postDetOnly(device, meta, senseTime) {
    const body = { deviceId: device };

    appendTimestamp(body, device, senseTime);

    const dataPayload = {};
    if (meta.system_voltage) dataPayload.system_voltage = meta.system_voltage;
    if (meta.ambient_light_lux) dataPayload.ambient_light_lux = meta.ambient_light_lux;
    if (meta.det) dataPayload.det = meta.det;
    body.data = dataPayload;

    console.log(`[${device}] Det body: ${JSON.stringify(body)}`);

    const headers = uploadHeaders ? JSON.parse(uploadHeaders) : {};
    headers['Content-Type'] = 'application/json';
    const resp = await fetch(uploadUrl, { method: 'POST', headers, body: JSON.stringify(body) });
    if (resp.ok) {
        console.log(`[${device}] Det uploaded to ${uploadUrl}`);
    } else {
        const text = await resp.text();
        console.error(`[${device}] Det upload failed (${resp.status}): ${text}`);
    }
}

function appendTimestamp(target, device, senseTime) {
    try {
        const st = new Date(senseTime);
        if (st instanceof Date && !isNaN(st)) {
            if (st.getFullYear() >= 2020) {
                if (typeof target.append === 'function') {
                    target.append('_timestamp', senseTime);  // FormData
                } else {
                    target._timestamp = senseTime;  // plain object
                }
            } else {
                console.log(`[${device}] Skipping _timestamp: year ${st.getFullYear()} is too far in the past`);
            }
        } else {
            console.log(`[${device}] Skipping _timestamp: failed to parse '${senseTime}'`);
        }
    } catch (e) {
        console.log(`[${device}] Skipping _timestamp: ${e.message}`);
    }
}

async function postComposite(device, meta, senseTime, includeDet, overlay) {
    const [jpegBuffer, detRaw] = await Promise.all([
        redisClient.GET(redis.commandOptions({ returnBuffers: true }), `ImageToRtsp:${device}:image:last`),
        redisClient.GET(`ImageToRtsp:${device}:det:last`),
    ]);
    if (!jpegBuffer) return;

    const detections = includeDet && overlay.bbox && detRaw ? JSON.parse(detRaw) : null;

    const image = sharp(jpegBuffer, { failOn: 'none' });
    const metadata = await image.metadata();
    const date = new Date(senseTime);
    const timestampStr = date.toLocaleString('sv-SE', { timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone });
    const composites = buildComposites(metadata, timestampStr, detections, overlay);
    const composed = await image.composite(composites).jpeg().toBuffer();

    const dataPayload = {};
    if (meta.system_voltage) dataPayload.system_voltage = meta.system_voltage;
    if (meta.ambient_light_lux) dataPayload.ambient_light_lux = meta.ambient_light_lux;
    if (includeDet && meta.det) dataPayload.det = meta.det;

    const form = new FormData();
    form.append('snap', new Blob([composed]), 'image.jpg');
    form.append('deviceId', device);
    appendTimestamp(form, device, senseTime);
    form.append('data', JSON.stringify(dataPayload));
    console.log(`[${device}] Composite data payload: ${JSON.stringify({...dataPayload, _timestamp: senseTime})}`);

    const headers = uploadHeaders ? JSON.parse(uploadHeaders) : {};
    const resp = await fetch(uploadUrl, { method: 'POST', headers, body: form });
    if (resp.ok) {
        console.log(`[${device}] Composite uploaded to ${uploadUrl}`);
    } else {
        const text = await resp.text();
        console.error(`[${device}] Composite upload failed (${resp.status}): ${text}`);
    }
}

async function doUploadDet(device) {
    if (!uploadUrl || detUploadMode < 2) return;
    const detRaw = await redisClient.GET(`ImageToRtsp:${device}:det`);
    if (!detRaw) return;
    const det = JSON.parse(detRaw);
    if (!det.length) return;

    const meta = { det };
    const senseTime = await redisClient.GET(`ImageToRtsp:${device}:sense_time`);
    await postDetOnly(device, meta, senseTime || "Unknown");
}

async function saveImages(device, senseTime, hasDet) {
    if (!saveDir) return;
    const jpegBuffer = await redisClient.GET(
        redis.commandOptions({ returnBuffers: true }),
        `ImageToRtsp:${device}:image:last`
    );
    if (!jpegBuffer) return;

    const ts = new Date(senseTime).toISOString().replace(/[:.]/g, '-');
    const filepath = path.join(saveDir, `${device}_${ts}_det.jpg`);

    if (hasDet && uploadOverlay.bbox) {
        const detRaw = await redisClient.GET(`ImageToRtsp:${device}:det:last`);
        const detections = detRaw ? JSON.parse(detRaw) : null;
        const image = sharp(jpegBuffer, { failOn: 'none' });
        const metadata = await image.metadata();
        const timestampStr = new Date(senseTime).toLocaleString('sv-SE', {
            timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone
        });
        const composites = buildComposites(metadata, timestampStr, detections, uploadOverlay);
        const composed = await image.composite(composites).jpeg().toBuffer();
        await fs.writeFile(filepath, composed);
    } else {
        await fs.writeFile(filepath, jpegBuffer);
    }
    console.log(`[${device}] Saved ${filepath}`);
}

async function doUpload(device) {
    if (!uploadUrl) return;
    const metaRaw = await redisClient.getDel(`ImageToRtsp:${device}:upload:ready`);
    if (!metaRaw) return;
    const meta = JSON.parse(metaRaw);
    const senseTime = meta.sense_time || "Unknown";
    const hasDet = meta.det && meta.det.length > 0;
    console.log(`[${device}] Upload triggered (mode=${detUploadMode}, overlay=${uploadOverlayStr})`);

    await saveImages(device, senseTime, hasDet).catch(e =>
        console.error(`[${device}] Save failed: ${e.message}`)
    );

    try {
        const includeDet = detUploadMode !== 3;
        await postComposite(device, meta, senseTime, includeDet, uploadOverlay);
    } catch (e) {
        console.error(`[${device}] Upload error: ${e.message}`);
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
        const device = path[0].toLowerCase();
        if (!HEX16_RE.test(device)) {
            res.statusCode = 400;
            res.end('Invalid device EUI');
            return;
        }
        const exists = await redisClient.EXISTS(`ImageToRtsp:${device}:image`);
        const lastExists = await redisClient.EXISTS(`ImageToRtsp:${device}:image:last`);
        if (!exists && !lastExists) {
            res.statusCode = 404;
            res.end('Device not found');
            return;
        }
        const isLastRequest = path.length === 2 && path[1] === 'last';
        
        let bufferKey = `ImageToRtsp:${device}:image`;
        let timestampKey = `ImageToRtsp:${device}:sense_time`;
        let detKey = params.get('det') === 'true' ? `ImageToRtsp:${device}:det` : null;

        if (isLastRequest) {
            bufferKey += ':last';
            timestampKey += ':last';
            if (detKey) detKey += ':last';
        }

        let mjpeg = params.get('mjpeg') !== 'false';

        if (mjpeg) {
            res.writeHead(200, {
                'Content-Type': 'multipart/x-mixed-replace;boundary="' + boundaryID + '"',
                'Connection': 'keep-alive',
                'Cache-Control': 'no-cache, no-store, max-age=0, must-revalidate',
                'Pragma': 'no-cache'
            });
            res.write('--' + boundaryID + '\r\n');

            // Send initial frame
            await fetchAndSendImage(res, bufferKey, timestampKey, true, detKey);

            // Setup Pub/Sub for live updates
            const subscriber = redisClient.duplicate();
            await subscriber.connect();
            
            const updateChannel = `EdgeEye:updated:${device}`;
            let heartbeatTimer = null;

            const sendHeartbeat = async () => {
                if (res.writableEnded) return;
                const size = await fetchAndSendImage(res, bufferKey, timestampKey, true, detKey);
                console.log(`[${device}] Heartbeat frame sent: ${size} bytes`);
                resetHeartbeat();
            };

            const resetHeartbeat = () => {
                if (heartbeatTimer) clearTimeout(heartbeatTimer);
                heartbeatTimer = setTimeout(sendHeartbeat, 10000); // 10s heartbeat
            };

            await subscriber.subscribe(updateChannel, async (message) => {
                if (message === "det") {
                    doUploadDet(device);
                } else {
                    const size = await fetchAndSendImage(res, bufferKey, timestampKey, true, detKey);
                    console.log(`[${device}] Updated frame sent: ${size} bytes`);
                    resetHeartbeat();
                    doUpload(device);
                }
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
            await fetchAndSendImage(res, bufferKey, timestampKey, false, detKey);
        }
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
