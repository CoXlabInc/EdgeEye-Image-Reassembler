"""Virtual LoRaWAN sessions for EdgeEye multi-session uplink.

For one image transfer the camera may use up to two extra sessions ("virtual
sessions") next to its own OTAA session. The reassembler generates their
DevAddr and session keys, hands them to the camera in an fPort 5 downlink and
decodes the frames sent on them itself, straight from the gateway bridge MQTT
topic (<prefix>/gateway/<gateway id>/event/up).

The virtual sessions are deliberately not registered in ChirpStack. ChirpStack
drops their frames as coming from an unknown device and never answers them,
whereas registered ABP devices get a MAC-command downlink (channel
configuration they never acknowledge) after every frame.

Frames are validated the way a network server validates a LoRaWAN 1.0.x
uplink: the DevAddr must belong to a session issued here, the MIC is checked
with the session NwkSKey, copies received by several gateways are processed
once and the FRMPayload is decrypted with the session AppSKey.

DevAddrs are drawn from a prefix outside the network's own NetID range
(default fe000000/7) so they never collide with a real device. The gateway
bridge must therefore not filter uplinks by NetID.
"""
import base64
import json
import secrets
import time

from cryptography.hazmat.primitives import cmac
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


# ---- gateway bridge message ------------------------------------------------

def _varint(buf, i):
    result = shift = 0
    while True:
        b = buf[i]
        i += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, i
        shift += 7


def phy_payload_of(message):
    """PHYPayload of a gateway bridge uplink message (protobuf or JSON marshaler), or None."""
    try:
        if message[:1] == b"{":
            pp = json.loads(message).get("phyPayload")
            return base64.b64decode(pp) if pp else None
        i, n = 0, len(message)
        while i < n:  # gw.UplinkFrame: field 1 (bytes) is phy_payload
            key, i = _varint(message, i)
            field, wire = key >> 3, key & 7
            if wire == 0:
                _, i = _varint(message, i)
            elif wire == 1:
                i += 8
            elif wire == 2:
                length, i = _varint(message, i)
                if field == 1:
                    return bytes(message[i:i + length])
                i += length
            elif wire == 5:
                i += 4
            else:
                return None
    except (ValueError, IndexError, TypeError):
        return None
    return None


# ---- LoRaWAN 1.0.x uplink ----------------------------------------------------

def parse_data_uplink(phy):
    """Split a LoRaWAN 1.0.x data uplink into its fields; None for anything else."""
    if len(phy) < 12 or (phy[0] >> 5) not in (2, 4) or (phy[0] & 0x03) != 0:
        return None
    fopts_len = phy[5] & 0x0F
    body_end = len(phy) - 4
    port_at = 8 + fopts_len
    if port_at > body_end:
        return None
    has_port = port_at < body_end
    return {
        "devaddr": int.from_bytes(phy[1:5], "little"),
        "fcnt": int.from_bytes(phy[6:8], "little"),
        "fport": phy[port_at] if has_port else None,
        "frm_payload": phy[port_at + 1:body_end] if has_port else b"",
        "msg": phy[:body_end],
        "mic": phy[body_end:],
    }


def uplink_mic(nwk_s_key, devaddr, fcnt, msg):
    b0 = (bytes([0x49, 0, 0, 0, 0, 0]) + devaddr.to_bytes(4, "little")
          + fcnt.to_bytes(4, "little") + bytes([0, len(msg)]))
    c = cmac.CMAC(algorithms.AES(nwk_s_key))
    c.update(b0 + msg)
    return c.finalize()[:4]


def uplink_crypt(app_s_key, devaddr, fcnt, data):
    """FRMPayload encryption of an uplink; applying it again decrypts."""
    enc = Cipher(algorithms.AES(app_s_key), modes.ECB()).encryptor()
    out = bytearray()
    for i in range(0, len(data), 16):
        a = (bytes([0x01, 0, 0, 0, 0, 0]) + devaddr.to_bytes(4, "little")
             + fcnt.to_bytes(4, "little") + bytes([0, i // 16 + 1]))
        s = enc.update(a)
        out += bytes(x ^ y for x, y in zip(data[i:i + 16], s))
    return bytes(out)


# ---- session bookkeeping -----------------------------------------------------

class SessionManager:
    PREFIX = "PP:EdgeEye"
    BOOKKEEPING_TTL = 24 * 3600   # session records must outlive any single image
    DEDUP_TTL = 120               # seconds a (DevAddr, FCnt) pair is remembered
    MAX_SESSIONS = 2              # the firmware holds at most two extra sessions

    def __init__(self, num_sessions=2, gateway_topic_prefixes="kr920", devaddr_prefix="fe000000/7"):
        self.num_sessions = max(1, min(int(num_sessions), self.MAX_SESSIONS))
        if isinstance(gateway_topic_prefixes, str):
            gateway_topic_prefixes = gateway_topic_prefixes.split(",")
        self.topic_prefixes = [p.strip().strip("/") for p in gateway_topic_prefixes if p.strip()]
        if not self.topic_prefixes:
            raise ValueError("at least one gateway topic prefix is required")
        try:
            base, bits = devaddr_prefix.split("/")
            bits = int(bits)
            base = int(base, 16)
        except ValueError:
            raise ValueError(f"DevAddr prefix must look like fe000000/7, got {devaddr_prefix!r}")
        if not 1 <= bits <= 24:
            raise ValueError(f"DevAddr prefix length must be 1..24 bits, got {bits}")
        self.addr_mask = (0xFFFFFFFF << (32 - bits)) & 0xFFFFFFFF
        self.addr_base = base & self.addr_mask
        self.devaddr_prefix = f"{self.addr_base:08x}/{bits}"

    # -- MQTT side

    @property
    def gateway_topics(self):
        return [f"{p}/gateway/+/event/up" for p in self.topic_prefixes]

    def is_gateway_topic(self, topic):
        return topic.endswith("/event/up") and any(topic.startswith(p + "/gateway/") for p in self.topic_prefixes)

    def owns(self, devaddr):
        return (devaddr & self.addr_mask) == self.addr_base

    def candidate(self, message):
        """Cheap pre-filter for the MQTT thread: the PHYPayload if it may be ours, else None."""
        phy = phy_payload_of(message)
        if not phy or len(phy) < 12 or (phy[0] >> 5) not in (2, 4):
            return None
        return phy if self.owns(int.from_bytes(phy[1:5], "little")) else None

    # -- lifecycle

    def _record_key(self, devaddr):
        return f"{self.PREFIX}:vsession:{devaddr:08x}"

    def _new_devaddr(self):
        return self.addr_base | (secrets.randbits(32) & ~self.addr_mask & 0xFFFFFFFF)

    async def create_for_image(self, r, parent_eui, epoch, num_sessions=None):
        """Issue the virtual sessions for one image.

        num_sessions overrides the default count (e.g. per device profile).
        Returns the fPort 5 payload the firmware expects:
        epoch (5 B LE) + per session [DevAddr 4 B LE][NwkSKey 16 B][AppSKey 16 B].
        """
        count = self.num_sessions if num_sessions is None else max(1, min(int(num_sessions), self.MAX_SESSIONS))
        await self.release(r, parent_eui)   # sessions of a previous image, if any
        payload = epoch.to_bytes(5, "little")
        created = []
        try:
            for index in range(1, count + 1):
                nwk_s_key = secrets.token_bytes(16)
                app_s_key = secrets.token_bytes(16)
                record = json.dumps({"parent": parent_eui, "index": index, "epoch": epoch,
                                     "nwk_s_key": nwk_s_key.hex(), "app_s_key": app_s_key.hex()})
                for _ in range(8):
                    devaddr = self._new_devaddr()
                    if await r.set(self._record_key(devaddr), record, ex=self.BOOKKEEPING_TTL, nx=True):
                        break
                else:
                    raise RuntimeError("no free DevAddr found in the virtual session prefix")
                created.append(devaddr)
                payload += devaddr.to_bytes(4, "little") + nwk_s_key + app_s_key
        except Exception:
            for devaddr in created:
                await r.delete(self._record_key(devaddr))
            raise
        await r.set(f"{self.PREFIX}:sessions:{parent_eui}",
                    json.dumps({"epoch": epoch, "devaddrs": [f"{a:08x}" for a in created], "ts": time.time()}),
                    ex=self.BOOKKEEPING_TTL)
        return payload

    async def release(self, r, parent_eui):
        """Forget the virtual sessions of a parent camera. Idempotent."""
        key = f"{self.PREFIX}:sessions:{parent_eui}"
        rec = await r.get(key)
        if not rec:
            return
        try:
            addrs = json.loads(rec).get("devaddrs", [])
        except ValueError:
            addrs = []
        for a in addrs:
            await r.delete(f"{self.PREFIX}:vsession:{a}")
        await r.delete(key)
        if addrs:
            print(f"[{parent_eui}] virtual sessions released ({', '.join(addrs)})")

    # -- uplinks

    async def decode_uplink(self, r, phy):
        """Validate and decrypt a frame sent on a virtual session.

        Returns {'parent', 'index', 'devaddr', 'f_cnt', 'f_port', 'raw'}, or None when the
        frame is not ours, fails the MIC check or is a copy received by another gateway.
        """
        f = parse_data_uplink(phy)
        if f is None or not self.owns(f["devaddr"]):
            return None
        rec = await r.get(self._record_key(f["devaddr"]))
        if not rec:
            return None  # not issued here, or already released
        s = json.loads(rec)
        devaddr, fcnt = f["devaddr"], f["fcnt"]
        # The firmware counts from 0 for every new session and an image needs far fewer
        # than 65536 frames per session, so the 16-bit FCnt on air is the whole counter.
        if uplink_mic(bytes.fromhex(s["nwk_s_key"]), devaddr, fcnt, f["msg"]) != f["mic"]:
            print(f"[{s['parent']}] virtual session {devaddr:08x}: MIC check failed (fCnt {fcnt}), frame dropped")
            return None
        if not await r.set(f"{self.PREFIX}:vseen:{devaddr:08x}:{fcnt}", 1, ex=self.DEDUP_TTL, nx=True):
            return None  # the same frame already arrived through another gateway
        if not f["fport"]:
            return None  # the firmware never sends MAC-only frames on these sessions
        return {"parent": s["parent"], "index": s["index"], "devaddr": f"{devaddr:08x}",
                "f_cnt": fcnt, "f_port": f["fport"],
                "raw": uplink_crypt(bytes.fromhex(s["app_s_key"]), devaddr, fcnt, f["frm_payload"])}
