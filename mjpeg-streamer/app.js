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

async function gatherDeviceData() {
    const deviceSet = new Set();
    let cursor = 0;
    do {
        const reply = await redisClient.SCAN(cursor, { MATCH: 'ImageToRtsp:*:sense_time', TYPE: 'string' });
        cursor = reply.cursor;
        for (const key of reply.keys) {
            const m = key.match(/^ImageToRtsp:([a-f0-9]+):sense_time$/);
            if (m) deviceSet.add(m[1]);
        }
    } while (cursor !== 0);

    const rows = (await Promise.all([...deviceSet].map(async (devEui) => {
        const [senseTime, hasImage, hasLast, activeEpoch] = await Promise.all([
            redisClient.GET(`ImageToRtsp:${devEui}:sense_time`),
            redisClient.EXISTS(`ImageToRtsp:${devEui}:image`),
            redisClient.EXISTS(`ImageToRtsp:${devEui}:image:last`),
            redisClient.GET(`PP:EdgeEye:active_epoch:${devEui}`),
        ]);

        let progress = null;
        if (activeEpoch) {
            const stateRaw = await redisClient.GET(`PP:EdgeEye:state:${devEui}:${activeEpoch}`);
            if (stateRaw) {
                try {
                    const st = JSON.parse(stateRaw);
                    if (st.total_size > 0) progress = Math.min(100, Math.round((st.received / st.total_size) * 100));
                } catch (_) {}
            }
        }
        return { devEui, senseTime, hasImage: !!hasImage, hasLast: !!hasLast, progress };
    }))).sort((a, b) => (b.senseTime || '').localeCompare(a.senseTime || ''));
    return rows;
}

async function handleRoot(res) {
    const rows = await gatherDeviceData();

    const esc = s => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');

    let html = `<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>EdgeEye Image Reassembler</title>
<style>
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;margin:20px;background:#fafafa}
h1{color:#333;font-weight:500}
table{border-collapse:collapse;width:100%;max-width:1000px;background:#fff;box-shadow:0 1px 3px rgba(0,0,0,.1);border-radius:4px}
th,td{text-align:left;padding:10px 14px;border-bottom:1px solid #eee}
th{background:#f5f5f5;font-weight:600;color:#555}
tr:hover{background:#f9f9f9}
.eui{font-family:monospace;font-size:.9em}
.progress-bar{background:#e0e0e0;border-radius:4px;height:14px;width:120px;display:inline-block;vertical-align:middle}
.progress-fill{background:#4caf50;height:14px;border-radius:4px}
.check{color:#4caf50;font-weight:bold}
.muted{color:#bbb}
a{color:#1976d2;text-decoration:none;margin-right:8px}
a:hover{text-decoration:underline}
.overlay{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.85);z-index:1000}
.overlay img{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);max-width:95vw;max-height:95vh;border-radius:4px;box-shadow:0 4px 20px rgba(0,0,0,.5)}
.overlay .close{position:absolute;top:16px;right:24px;color:#fff;font-size:36px;cursor:pointer;line-height:1;font-weight:bold}
</style></head><body>
<h1>EdgeEye Image Reassembler</h1>
<table id="devices"><thead><tr><th>DevEUI</th><th>Last Activity</th><th>Reassembly</th><th>View</th></tr></thead><tbody>`;

    html += buildRows(rows, esc);

    html += `</tbody></table>

<div id="popup" class="overlay" onclick="closePopup()">
  <span class="close">&times;</span>
  <img id="popupImg" src="" alt="">
</div>

<script>
function showPopup(url){
  document.getElementById('popupImg').src=url;
  document.getElementById('popup').style.display='block';
}
function closePopup(){
  document.getElementById('popupImg').src='';
  document.getElementById('popup').style.display='none';
}
(function(){
var es=new EventSource('/stream');
es.onmessage=function(e){
  try{var d=JSON.parse(e.data);
  var tb=document.querySelector('#devices tbody');
  if(tb)tb.innerHTML=d.html;
  }catch(x){}
};
es.onerror=function(){var t=setTimeout(function(){es.close()},3e4)};
})();
</script>
</body></html>`;
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
}

// Reusable row builder — used by both SSR and SSE
function buildRows(rows, esc) {
    if (rows.length === 0) {
        return '<tr><td colspan="4" style="text-align:center;color:#999;padding:24px">No devices found</td></tr>';
    }
    let html = '';
    for (const r of rows) {
        const pct = r.progress !== null
            ? '<div class="progress-bar"><div class="progress-fill" style="width:' + r.progress + '%"></div></div> ' + r.progress + '%'
            : '<span class="muted">&mdash;</span>';
        const ts = r.senseTime ? esc(r.senseTime) : '<span class="muted">&mdash;</span>';
        html += '<tr>\n<td class="eui">' + r.devEui + '</td>\n' +
            '<td>' + ts + '</td>\n' +
            '<td>' + pct + '</td>\n' +
            '<td><a href="#" onclick="showPopup(this.dataset.url)" data-url="/' + r.devEui + '">Live</a><a href="#" onclick="showPopup(this.dataset.url)" data-url="/' + r.devEui + '/last">Last</a></td>\n' +
            '</tr>';
    }
    return html;
}

async function handleStream(res) {
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
    });
    res.write('\n');

    const subscriber = redisClient.duplicate();
    await subscriber.connect();

    const esc = s => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');

    const send = async () => {
        if (res.writableEnded) return;
        try {
            const data = await gatherDeviceData();
            const html = buildRows(data, esc);
            res.write('data: ' + JSON.stringify({ html }) + '\n\n');
        } catch (e) {
            console.error('SSE send error:', e.message);
        }
    };

    await send();

    await subscriber.pSubscribe('EdgeEye:updated:*', async () => {
        await send();
    });

    const heartbeat = setInterval(() => {
        if (res.writableEnded) { clearInterval(heartbeat); return; }
        res.write(': hb\n\n');
    }, 30000);

    res.on('close', async () => {
        clearInterval(heartbeat);
        try {
            await subscriber.pUnsubscribe();
            await subscriber.disconnect();
        } catch (_) {}
    });
}

/**
 * create a server to serve out the motion jpeg images
 */
var server = http.createServer(async (req, res) => {
    let uri = req.url.split('?');
    let path = uri[0].split('/').slice(1);
    let params = new URLSearchParams(uri[1]);
    
    if (path.length === 0 || (path.length === 1 && path[0] === '')) {
        await handleRoot(res);
    } else if (path.length === 1 && path[0] === 'stream') {
        await handleStream(res);
    } else if (path.length >= 1 && path[0] !== 'healthcheck' && path[0] !== '') {
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
