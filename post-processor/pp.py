import argparse
from urllib.parse import urlparse
import EdgeEye
import sys

if __name__ == '__main__':
    app_desc = "EdgeEye Post Processor (Chirpstack v4 MQTT)"

    parser = argparse.ArgumentParser(description=app_desc)
    parser.add_argument("--mqtt_url", help="Chirpstack MQTT broker URL", required=True)
    parser.add_argument("--mqtt_user", help="MQTT username", required=False, default=None)
    parser.add_argument("--mqtt_pass", help="MQTT password", required=False, default=None)
    parser.add_argument("--redis_url", help="Redis URL for context storage", required=True)
    parser.add_argument("--device_profile_id", help="Filter by Device Profile ID", required=True)
    args = parser.parse_args()

    print(app_desc)
    
    if not args.device_profile_id:
        print("Error: --device_profile_id is required. Use -h for help.")
        sys.exit(1)

    mqtt_info = {
        'url': args.mqtt_url.strip(),
        'user': args.mqtt_user.strip() if args.mqtt_user else None,
        'pass': args.mqtt_pass.strip() if args.mqtt_pass else None
    }

    print(f"MQTT Broker: {mqtt_info['url']}")
    print(f"Filtering by Device Profile ID: {args.device_profile_id}")
    
    url_parsed = urlparse(args.redis_url)
    print(f"Redis: {url_parsed.scheme}://{url_parsed.hostname}" + (f":{url_parsed.port}" if url_parsed.port is not None else ""))
    
    # Initialize EdgeEye and start the loop
    EdgeEye.init(mqtt_info, args.redis_url.strip(), args.device_profile_id.strip()).loop_forever()
