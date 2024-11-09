import argparse
from urllib.parse import urlparse

import EdgeEye

import pyiotown.post
import pyiotown.post_process
import pyiotown.get

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

url = None
dry_run = False

if __name__ == '__main__':
    app_desc = "EdgeEye Post Processor"

    parser = argparse.ArgumentParser(description=app_desc)
    parser.add_argument("--url", help="IOTOWN URL", required=True)
    parser.add_argument("--mqtt_url", help="MQTT broker URL for IoT.own", required=False, default=None)
    parser.add_argument("--redis_url", help="Redis URL for context storage", required=True)
    parser.add_argument("--chirpstack_url", help="Chirpstack Application Server URL for performance enhancement", required=False, default=None)
    parser.add_argument("--chirpstack_jwt_secret", help="JWT secret for Chirpstack Application Server", required=False, default=None)
    parser.add_argument('--dry', help=" Do not upload data to the server", type=int, default=0)
    args = parser.parse_args()

    print(app_desc)
    url = args.url.strip()
    url_parsed = urlparse(url)

    print(f"IOTOWN: {url_parsed.scheme}://{url_parsed.hostname}" + (f":{url_parsed.port}" if url_parsed.port is not None else ""))

    mqtt_url = args.mqtt_url.strip() if args.mqtt_url is not None else None
    chirpstack = {
        'url': args.chirpstack_url.strip() if args.chirpstack_url is not None else None,
        'secret': args.chirpstack_jwt_secret.strip() if args.chirpstack_jwt_secret is not None else None
    }

    if args.dry == 1:
        dry_run = True
        print("DRY RUNNING!")

    url_parsed = urlparse(args.redis_url)
    print(f"Redis: {url_parsed.scheme}://{url_parsed.hostname}" + (f":{url_parsed.port}" if url_parsed.port is not None else ""))
    EdgeEye.init(url, 'EdgeEye', mqtt_url, args.redis_url.strip(), chirpstack=chirpstack, dry_run=dry_run).loop_forever()
