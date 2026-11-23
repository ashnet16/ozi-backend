"""Seed the local Snapchain devnet with casts, reactions and removals.

Run it after `make snapchain-up` and `make setup`, while the poller is running
(the poller only receives new events). `make devnet-seed` installs the
dependencies and runs it; see the readme's Developer Guide.

What it does:
  1. Registers three test FIDs through `fc devnet bootstrap` (inside node1), all
     sharing one signer key. FIDs that already have that signer are skipped, so
     the script can be re-run.
  2. Builds and signs messages itself (`fc` cannot send reactions) and submits
     them to node1's HTTP API, in phases with a pause between each:
       adds      casts and replies
       reactions likes and recasts
       removals  cast and reply deletes and reaction removes, including edge cases
       late adds a cast that already has a like, a cast that was already
                 deleted, and a like re-added after it was removed
  3. Prints the state Ozi should end up in, and writes it as JSON with --manifest.

Every run uses fresh cast texts, so each run adds a new, independent set.
"""

import argparse
import json
import os
import secrets
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import blake3
from nacl.signing import SigningKey

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "proto"))

import message_pb2 as pb  # noqa: E402

FARCASTER_EPOCH = 1_609_459_200  # seconds; 2021-01-01T00:00:00Z
# Not a secret: a fixed devnet-only key so re-runs reuse the registered signer.
DEFAULT_SIGNER_SECRET = "0x" + "0d" * 32
SNAPCHAIN_COMPOSE = ROOT / "snapchain" / "docker-compose.yml"
LIKE = pb.REACTION_TYPE_LIKE
RECAST = pb.REACTION_TYPE_RECAST


class Node:
    def __init__(self, http_url):
        self.http_url = http_url.rstrip("/")

    def get(self, path):
        with urllib.request.urlopen(self.http_url + path, timeout=10) as resp:
            return resp.read().decode()

    def submit(self, msg):
        """POST a protobuf Message. Returns (ok, response text)."""
        req = urllib.request.Request(
            self.http_url + "/v1/submitMessage",
            data=msg.SerializeToString(),
            headers={"content-type": "application/octet-stream"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return True, resp.read().decode()
        except urllib.error.HTTPError as e:
            return False, e.read().decode()

    def has_signer(self, fid, pubkey_hex):
        try:
            body = self.get(f"/v1/onChainSignersByFid?fid={fid}")
        except urllib.error.URLError:
            return False
        return pubkey_hex.lower() in body.lower()


class Signer:
    def __init__(self, secret_hex):
        self.key = SigningKey(bytes.fromhex(secret_hex.removeprefix("0x")))
        self.pubkey = bytes(self.key.verify_key)

    def message(self, data):
        data_bytes = data.SerializeToString()
        msg_hash = blake3.blake3(data_bytes).digest()[:20]
        return pb.Message(
            data=data,
            hash=msg_hash,
            hash_scheme=pb.HASH_SCHEME_BLAKE3,
            signature=self.key.sign(msg_hash).signature,
            signature_scheme=pb.SIGNATURE_SCHEME_ED25519,
            signer=self.pubkey,
        )


def now():
    """The current Farcaster time, in seconds. Messages built in the same second
    share a timestamp, which is fine unless one undoes the other: a reaction
    remove must be later than its add. The --pause of at least 1s between the
    reactions and removals phases guarantees that."""
    return int(time.time()) - FARCASTER_EPOCH


def data(fid, msg_type, ts, **body):
    return pb.MessageData(
        type=msg_type, fid=fid, timestamp=ts, network=pb.FARCASTER_NETWORK_DEVNET, **body
    )


def cast_add(fid, ts, text, parent=None):
    body = pb.CastAddBody(text=text)
    if parent is not None:
        body.parent_cast_id.CopyFrom(parent)
    return data(fid, pb.MESSAGE_TYPE_CAST_ADD, ts, cast_add_body=body)


def cast_remove(fid, ts, target_hash):
    return data(
        fid, pb.MESSAGE_TYPE_CAST_REMOVE, ts,
        cast_remove_body=pb.CastRemoveBody(target_hash=target_hash),
    )


def reaction(fid, ts, kind, target, remove=False):
    msg_type = pb.MESSAGE_TYPE_REACTION_REMOVE if remove else pb.MESSAGE_TYPE_REACTION_ADD
    body = pb.ReactionBody(type=kind, target_cast_id=target)
    return data(fid, msg_type, ts, reaction_body=body)


def cast_id(msg):
    return pb.CastId(fid=msg.data.fid, hash=msg.hash)


def bootstrap(node, fids, signer_secret, signer_pubkey_hex, auth):
    todo = [f for f in fids if not node.has_signer(f, signer_pubkey_hex)]
    if not todo:
        print(f"FIDs {fids} already registered with the seed signer.")
        return
    for fid in todo:
        print(f"Registering fid {fid} ...")
        subprocess.run(
            [
                "docker", "compose", "-f", str(SNAPCHAIN_COMPOSE), "exec", "-T", "node1",
                "./fc", "devnet", "bootstrap", "--network", "devnet",
                "--node", "http://localhost:3381", "--grpc-node", "http://localhost:3383",
                "--auth", auth, "--fid", str(fid), "--signer-secret", signer_secret,
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
    # On-chain events land a few blocks later; messages are rejected until then.
    deadline = time.time() + 60
    while not all(node.has_signer(f, signer_pubkey_hex) for f in todo):
        if time.time() > deadline:
            sys.exit(f"Signers for {todo} did not appear on the node within 60s.")
        time.sleep(1)
    print(f"Registered {todo}.")


def poller_running():
    try:
        out = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", "ozi-poller"],
            capture_output=True, text=True,
        )
        return out.stdout.strip() == "true"
    except FileNotFoundError:
        return False


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--node", default=os.getenv("SNAPCHAIN_HTTP", "http://localhost:3381"),
                   help="node1 HTTP API (default %(default)s)")
    p.add_argument("--fid", type=int, default=2001,
                   help="first of three consecutive test FIDs (default %(default)s)")
    p.add_argument("--auth", default="dev:dev", help="devnet admin RPC auth")
    p.add_argument("--pause", type=float, default=3.0,
                   help="seconds between phases, at least 1 (default %(default)s)")
    p.add_argument("--manifest", type=Path, help="also write the expected end state as JSON here")
    p.add_argument("--no-poller-check", action="store_true",
                   help="don't require the ozi-poller container to be running")
    args = p.parse_args()
    if args.pause < 1:
        p.error("--pause must be at least 1 second, so removals are timestamped after their adds")

    if not args.no_poller_check and not poller_running():
        sys.exit("ozi-poller is not running, so it would miss these events. "
                 "Run `make setup` first, or pass --no-poller-check.")

    node = Node(args.node)
    try:
        node.get("/v1/info")
    except urllib.error.URLError as e:
        sys.exit(f"Snapchain node not reachable at {args.node} ({e.reason}). Run `make snapchain-up`.")

    secret = os.getenv("SIGNER_SECRET", DEFAULT_SIGNER_SECRET)
    signer = Signer(secret)
    a, b, c = args.fid, args.fid + 1, args.fid + 2
    bootstrap(node, [a, b, c], secret, signer.pubkey.hex(), args.auth)

    run = secrets.token_hex(3)
    failures = []
    log = []

    def submit(label, msg, expect_reject=False):
        ok, resp = node.submit(msg)
        as_expected = ok != expect_reject
        note = ""
        if expect_reject:
            note = "  (expected)" if as_expected else "  (expected a rejection)"
        print(f"  {'ok' if ok else 'rejected':8} {label:44} 0x{msg.hash.hex()}{note}")
        if not as_expected:
            failures.append((label, resp))
            if not ok:
                print(f"           {resp[:300]}")
        log.append({"label": label, "hash": msg.hash.hex(), "fid": msg.data.fid,
                    "type": pb.MessageType.Name(msg.data.type), "accepted": ok})
        return msg

    def send(label, msg_data):
        return submit(label, signer.message(msg_data))

    def text(n):
        return f"[seed {run}] {n}"

    # Built up front so a reaction can target a cast before the cast is sent.
    late = signer.message(cast_add(a, now(), text("A: cast liked before it existed")))
    ghost = signer.message(cast_add(a, now(), text("A: cast deleted before it was sent")))

    print(f"\nPhase 1: adds (run {run})")
    c1 = send("A cast c1 (kept)", cast_add(a, now(), text("A: farcaster devnet hello")))
    c2 = send("A cast c2 (deleted later)", cast_add(a, now(), text("A: this cast will be deleted")))
    c3 = send("A cast c3 (deleted, no reactions)", cast_add(a, now(), text("A: short-lived cast")))
    c4 = send("B cast c4 (kept)", cast_add(b, now(), text("B: snapchain removals test")))
    c5 = send("C cast c5 (kept)", cast_add(c, now(), text("C: just a regular cast")))
    r1 = send("B reply r1 to c1 (kept)", cast_add(b, now(), text("B: replying to A"), parent=cast_id(c1)))
    r2 = send("C reply r2 to c1 (deleted later)", cast_add(c, now(), text("C: reply that will be deleted"), parent=cast_id(c1)))
    time.sleep(args.pause)

    print("\nPhase 2: reactions")
    send("B likes c1", reaction(b, now(), LIKE, cast_id(c1)))
    send("C likes c1 (removed later)", reaction(c, now(), LIKE, cast_id(c1)))
    send("C recasts c1", reaction(c, now(), RECAST, cast_id(c1)))
    send("A likes c4", reaction(a, now(), LIKE, cast_id(c4)))
    send("B likes c4 (removed, then re-added)", reaction(b, now(), LIKE, cast_id(c4)))
    send("B recasts c2 (removed later)", reaction(b, now(), RECAST, cast_id(c2)))
    send("C likes c2", reaction(c, now(), LIKE, cast_id(c2)))
    send("C likes c_late before it is cast", reaction(c, now(), LIKE, cast_id(late)))
    time.sleep(args.pause)

    print("\nPhase 3: removals")
    send("A deletes c2", cast_remove(a, now(), c2.hash))
    send("A deletes c3", cast_remove(a, now(), c3.hash))
    send("C deletes reply r2", cast_remove(c, now(), r2.hash))
    send("C removes like on c1", reaction(c, now(), LIKE, cast_id(c1), remove=True))
    send("B removes like on c4", reaction(b, now(), LIKE, cast_id(c4), remove=True))
    send("B removes recast on c2 (deleted cast)", reaction(b, now(), RECAST, cast_id(c2), remove=True))
    send("C removes like on c4 it never added", reaction(c, now(), LIKE, cast_id(c4), remove=True))
    send("A deletes c_ghost before sending it", cast_remove(a, now(), ghost.hash))
    time.sleep(args.pause)

    # After the removals phase's pause, so this like is timestamped after its removal.
    print("\nPhase 4: late adds")
    submit("A cast c_late (already has a like)", late)
    submit("A cast c_ghost (already deleted)", ghost, expect_reject=True)
    send("B likes c4 again after removing the like", reaction(b, now(), LIKE, cast_id(c4)))

    expected = {
        "run": run,
        "fids": {"A": a, "B": b, "C": c},
        "visible_casts": {
            "c1": {"hash": c1.hash.hex(), "likes": 1, "recasts": 1, "replies": 1},
            "c4": {"hash": c4.hash.hex(), "likes": 2, "recasts": 0},
            "c5": {"hash": c5.hash.hex(), "likes": 0, "recasts": 0},
            "c_late": {"hash": late.hash.hex(), "likes": 1, "recasts": 0},
        },
        "visible_replies": {"r1": {"hash": r1.hash.hex(), "parent": "c1"}},
        "deleted_casts": {"c2": c2.hash.hex(), "c3": c3.hash.hex(), "c_ghost": ghost.hash.hex()},
        "deleted_replies": {"r2": r2.hash.hex()},
        "messages": log,
    }

    print(f"""
Expected state in Ozi once the removals feature works (texts contain "[seed {run}]"):
  visible   c1      likes 1 (B), recasts 1 (C), 1 reply (r1); r2 was deleted
  visible   c4      likes 2 (A, B)  B's like was removed, then added again;
                    C's unlike of a like it never made changes nothing
  visible   c5      no reactions
  visible   c_late  likes 1 (C)   the like was sent before the cast
  visible   r1      reply to c1
  gone      r2      reply to c1, deleted (comments table)
  gone      c2      deleted; its reactions should no longer count
  gone      c3      deleted
  never     c_ghost Snapchain rejects it, so Ozi only ever sees its CAST_REMOVE

Check Postgres:
  docker exec ozi-postgres psql -U $POSTGRES_USER -d ozi -c \\
    "select fid, text from casts where text like '[seed {run}]%' order by msg_timestamp"
  docker exec ozi-postgres psql -U $POSTGRES_USER -d ozi -c \\
    "select target_hash_hex, reaction_type, count(*) from reactions where fid in ({a},{b},{c}) group by 1, 2"
""")

    if args.manifest:
        args.manifest.parent.mkdir(parents=True, exist_ok=True)
        args.manifest.write_text(json.dumps(expected, indent=2))
        print(f"Wrote {args.manifest}")

    if failures:
        print(f"{len(failures)} submission(s) did not behave as expected:")
        for label, resp in failures:
            print(f"  {label}: {resp[:300]}")
        sys.exit(1)


if __name__ == "__main__":
    main()
