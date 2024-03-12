#!/usr/bin/env node

import os from 'os';
import http from 'http';
import sharp from 'sharp';

import { Command } from 'commander';
const program = new Command();

import redis from 'redis';
import pjson from './package.json' assert { type: 'json' }

program
    .version(pjson.version)
    .description(pjson.description)
    .option('-r --redis <URL>', 'Redis URL (default redis://localhost)')
    .option('-p --port <n>', 'port number (default 8080)', parseInt)
    .option('-v --version', 'show version')
    .parse(process.argv);

const opts = program.opts();

var port = opts.port || 8080,
    boundaryID = "COXLABBOUNDARY";

const redisUrl = opts.redis || 'redis://localhost';

console.log(`Connecting Redis (${redisUrl})`);

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

async function sendAnImage(res, bufferKey, lastOffset) {
    if (res.writableEnded) {
        return;
    }
    
    let buffer = await redisClient.GET(redis.commandOptions({
        returnBuffers: true
    }), bufferKey);
    
    if (buffer.length > 0) {
        res.write('Content-Type: image/jpeg\r\n');
	try {
	    buffer = await sharp(buffer, { failOn: 'none' }).jpeg().toBuffer();
	} catch(e) {
            console.error(e);
            buffer = await redisClient.GET(redis.commandOptions({
                returnBuffers: true
            }), bufferKey + ':last');

            try {
                buffer = await sharp(buffer, { failOn: 'none' }).jpeg().toBuffer();
            } catch(e) {
                console.error(e);
            }
	}

        res.write(`Content-Length: ${buffer.length}\r\n\r\n`);
        res.write(buffer, 'binary');
        console.log(`Load and write data ${buffer.length}`);
        res.write('\r\n--' + boundaryID + '\r\n');
    }

    setTimeout(sendAnImage,
               (lastOffset === buffer.length) ? 1000 : 100,
               res, bufferKey, buffer.length);
};

/**
 * create a server to serve out the motion jpeg images
 */
var server = http.createServer(async (req, res) => {
    console.log(`Req URL: ${req.url}`);
    // return a html page if the user accesses the server directly
    if (req.url === "/") {
        res.writeHead(200, { "content-type": "text/html;charset=utf-8" });
        res.write('<!doctype html>');
        res.write('<html>');
        res.write('<head><title>' + pjson.name + '</title><meta charset="utf-8" /></head>');
        res.write('<body>');
        res.write('<img src="/LWAC1F09FFFE09112A/image" />');
        res.write('</body>');
        res.write('</html>');
        res.end();
        return;
    }

    if (req.url === "/healthcheck") {
        res.statusCode = 200;
        res.end();
        return;
    };

    // for image requests, return a HTTP multipart document (stream)
    let uri = req.url.split('/').slice(1);

    if (uri.length == 2) {
        const device = uri[0];
        const key = uri[1];
        const bufferKey = `ImageToRtsp:${device}:${key}:buffer`;
        
        let bufferLength = await redisClient.STRLEN(bufferKey);
        if (bufferLength == 0) {
            res.statusCode = 404;
            res.write('No image streaming found');
            res.end();
            return;
        } else {
            res.writeHead(200, {
                'Content-Type': 'multipart/x-mixed-replace;boundary="' + boundaryID + '"',
                'Connection': 'keep-alive',
                'Expires': 'Fri, 27 May 1977 00:00:00 GMT',
                'Cache-Control': 'no-cache, no-store, max-age=0, must-revalidate',
                'Pragma': 'no-cache'
            });
            console.log('writing header');

            res.write('--' + boundaryID + '\r\n');
            await sendAnImage(res, bufferKey);
        }

        res.on('close', function() {
            res.end();
        });
    } else {
        res.statusCode = 404;
        res.end();
        return;
    }
});

server.on('error', function(e) {
    if (e.code == 'EADDRINUSE') {
        console.log('port already in use');
    } else if (e.code == "EACCES") {
        console.log("Illegal port");
    } else {
        console.log("Unknown error");
    }
    process.exit(1);
});

// start the server
server.listen(port);
console.log(pjson.name + " started on port " + port);
