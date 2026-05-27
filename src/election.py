"""
Bully Algorithm for Leader Election (Garcia-Molina, 1982).

Each node has a unique integer ID (from NODE_ID env var). Higher ID = higher priority.
Nodes communicate via HTTP POST to peer endpoints.

Protocol:
- Election initiation: send ELECTION to all higher-ID peers
- If no higher peer responds: declare victory, send COORDINATOR to all
- If a higher peer responds ALIVE: defer; that peer takes over the election
- Leader sends periodic heartbeat; followers monitor and trigger election on timeout
- On receiving ELECTION from a lower-ID node: respond ALIVE, then start own election

Configuration env vars:
    NODE_ID              - This node's bully rank (int, default 1)
    PEERS                - Comma-separated peer URLs (e.g. "http://node-2:8080,http://node-3:8080")
    HEARTBEAT_INTERVAL   - Seconds between heartbeats (int, default 5)
    ELECTION_TIMEOUT     - Seconds to wait for peer response (int, default 3)
    LEADER_TIMEOUT       - Seconds without heartbeat before triggering election (int, default 10)
"""

import logging
import os
import re
import threading
import time
from typing import Optional

import requests

logger = logging.getLogger("election")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

NODE_ID = int(os.environ.get("NODE_ID", "1"))
PEERS = os.environ.get("PEERS", "")
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "5"))
ELECTION_TIMEOUT = int(os.environ.get("ELECTION_TIMEOUT", "3"))
LEADER_TIMEOUT = int(os.environ.get("LEADER_TIMEOUT", "10"))

# ---------------------------------------------------------------------------
# Global state (thread-safe via _lock)
# ---------------------------------------------------------------------------

current_leader_id: Optional[int] = None
current_leader_url: Optional[str] = None
_last_heartbeat: float = 0.0
_election_in_progress: bool = False
_lock = threading.Lock()
_running: bool = False

# ---------------------------------------------------------------------------
# Peer management
# ---------------------------------------------------------------------------


def _extract_id(url: str) -> Optional[int]:
    """Extract node ID from a URL like http://node-3:8080."""
    match = re.search(r"node-(\d+)", url)
    return int(match.group(1)) if match else None


def get_peers() -> list[tuple[int, str]]:
    """Return list of (id, url) for all configured peers."""
    if not PEERS:
        return []
    result: list[tuple[int, str]] = []
    for p in PEERS.split(","):
        p = p.strip()
        if p:
            pid = _extract_id(p)
            if pid is not None:
                result.append((pid, p))
    return result


def get_higher_peers() -> list[tuple[int, str]]:
    """Peers with ID > NODE_ID (potential election targets)."""
    return [(pid, url) for pid, url in get_peers() if pid > NODE_ID]


def _peer_url_by_id(peer_id: int) -> Optional[str]:
    """Look up a peer's URL by its ID."""
    for pid, url in get_peers():
        if pid == peer_id:
            return url
    return None


# ---------------------------------------------------------------------------
# Core election logic
# ---------------------------------------------------------------------------


def start_election() -> bool:
    """
    Initiate a Bully election.

    Sends ELECTION to all higher-ID peers. If none respond, declares victory.
    If any respond ALIVE, defers and waits for their COORDINATOR announcement.

    Returns:
        True if this node became the leader.
    """
    global _election_in_progress, current_leader_id, current_leader_url

    with _lock:
        if _election_in_progress:
            logger.debug("Node %d: election already in progress, skipping", NODE_ID)
            return False
        _election_in_progress = True

    try:
        logger.info("Node %d: starting election", NODE_ID)
        higher = get_higher_peers()

        if not higher:
            # No higher-ID nodes exist — win immediately.
            declare_victory()
            return True

        # Send ELECTION to every higher peer.
        alive_responses: list[int] = []
        for peer_id, peer_url in higher:
            try:
                resp = requests.post(
                    f"{peer_url}/election",
                    json={"sender_id": NODE_ID},
                    timeout=ELECTION_TIMEOUT,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("response") == "alive":
                        alive_responses.append(peer_id)
                        logger.info(
                            "Node %d: higher node %d responded alive", NODE_ID, peer_id
                        )
            except requests.RequestException:
                logger.info(
                    "Node %d: higher node %d did not respond (timeout/error)",
                    NODE_ID,
                    peer_id,
                )

        if alive_responses:
            # At least one higher node is alive — it will take over.
            logger.info(
                "Node %d: deferring to higher nodes %s, waiting for coordinator",
                NODE_ID,
                alive_responses,
            )
            _wait_for_coordinator()
            return False
        else:
            # No higher node answered — we are the new leader.
            declare_victory()
            return True
    finally:
        with _lock:
            _election_in_progress = False


def _wait_for_coordinator() -> None:
    """
    Block until a COORDINATOR message arrives or LEADER_TIMEOUT expires.

    If the timeout expires without receiving a coordinator announcement,
    restart the election (the higher node may have crashed mid-election).
    """
    deadline = time.time() + LEADER_TIMEOUT
    while time.time() < deadline:
        if current_leader_id is not None:
            return  # Coordinator received.
        time.sleep(0.5)

    logger.warning(
        "Node %d: no coordinator received after %.0fs, restarting election",
        NODE_ID,
        LEADER_TIMEOUT,
    )
    # The higher node that responded ALIVE may have crashed.
    # Restart the election.
    # We don't call start_election() directly here because we're inside
    # the lock-protected block of the original start_election().
    # The caller's finally block will release _election_in_progress,
    # so we spawn a new thread to restart.
    threading.Thread(target=start_election, daemon=True).start()


def handle_election_message(sender_id: int) -> dict:
    """
    Handle an incoming ELECTION message from *sender_id*.

    Called by the /election API endpoint.

    If sender_id < NODE_ID:
        - Respond ALIVE immediately.
        - Start own election in a background thread (to cascade upward).
    If sender_id >= NODE_ID:
        - This should not happen in a correct Bully run, but respond gracefully.

    Returns a dict suitable as a JSON response.
    """
    if sender_id < NODE_ID:
        logger.info(
            "Node %d: received ELECTION from %d, responding alive", NODE_ID, sender_id
        )
        # If we are already the leader, just acknowledge and resend COORDINATOR
        # so the sender knows who the leader is.
        if current_leader_id == NODE_ID:
            # Re-announce leadership to the sender specifically.
            sender_url = _peer_url_by_id(sender_id)
            if sender_url:
                _notify_coordinator(sender_id, sender_url)
            return {"response": "alive", "node_id": NODE_ID}

        # Start our own election in the background.
        threading.Thread(target=start_election, daemon=True).start()
        return {"response": "alive", "node_id": NODE_ID}
    else:
        logger.warning(
            "Node %d: received ELECTION from higher/equal node %d, ignoring",
            NODE_ID,
            sender_id,
        )
        return {"response": "ignored"}


def handle_coordinator_message(leader_id: int, leader_url: str) -> None:
    """
    Handle an incoming COORDINATOR message.

    Records the sender as the new leader and resets the heartbeat timer.
    """
    global current_leader_id, current_leader_url, _last_heartbeat
    with _lock:
        current_leader_id = leader_id
        current_leader_url = leader_url
        _last_heartbeat = time.time()
    logger.info("Node %d: new leader is %d (%s)", NODE_ID, leader_id, leader_url)


def handle_heartbeat(leader_id: int) -> None:
    """
    Handle an incoming heartbeat from the leader.

    Refreshes the heartbeat timestamp if the sender matches the current leader.
    """
    global _last_heartbeat
    if current_leader_id == leader_id:
        _last_heartbeat = time.time()


def declare_victory() -> None:
    """
    Declare this node as the leader and notify all peers.
    """
    global current_leader_id, current_leader_url, _last_heartbeat
    with _lock:
        current_leader_id = NODE_ID
        current_leader_url = f"http://node-{NODE_ID}:8080"
        _last_heartbeat = time.time()
    logger.info("Node %d: declaring victory — I am the leader", NODE_ID)

    for peer_id, peer_url in get_peers():
        _notify_coordinator(peer_id, peer_url)


def _notify_coordinator(peer_id: int, peer_url: str) -> None:
    """Send COORDINATOR message to a single peer."""
    try:
        requests.post(
            f"{peer_url}/coordinator",
            json={
                "leader_id": NODE_ID,
                "leader_url": f"http://node-{NODE_ID}:8080",
            },
            timeout=ELECTION_TIMEOUT,
        )
    except requests.RequestException:
        logger.warning(
            "Node %d: failed to notify peer %d of victory", NODE_ID, peer_id
        )


# ---------------------------------------------------------------------------
# Leader state query
# ---------------------------------------------------------------------------


def get_leader_info() -> dict:
    """Return the current leader info (or nulls if no leader)."""
    with _lock:
        return {
            "leader_id": current_leader_id,
            "leader_url": current_leader_url,
        }


# ---------------------------------------------------------------------------
# Background threads
# ---------------------------------------------------------------------------


def _leader_heartbeat_loop() -> None:
    """Background thread: if this node is leader, send periodic heartbeats."""
    global _running
    while _running:
        if current_leader_id == NODE_ID:
            for peer_id, peer_url in get_peers():
                try:
                    requests.post(
                        f"{peer_url}/heartbeat",
                        json={"leader_id": NODE_ID},
                        timeout=2,
                    )
                except requests.RequestException:
                    pass
        time.sleep(HEARTBEAT_INTERVAL)


def _heartbeat_monitor_loop() -> None:
    """Background thread: monitor leader liveness, trigger election on timeout."""
    global _running, current_leader_id, current_leader_url
    while _running:
        if current_leader_id is not None and current_leader_id != NODE_ID:
            elapsed = time.time() - _last_heartbeat
            if elapsed > LEADER_TIMEOUT:
                logger.warning(
                    "Node %d: leader %d timed out (no heartbeat for %.1fs)",
                    NODE_ID,
                    current_leader_id,
                    elapsed,
                )
                with _lock:
                    current_leader_id = None
                    current_leader_url = None
                threading.Thread(target=start_election, daemon=True).start()
        time.sleep(HEARTBEAT_INTERVAL)


def _startup_election_check() -> None:
    """
    One-shot startup check: query peers for an existing leader.

    If a leader is found, record it. Otherwise, start an election.
    Runs in a background thread to avoid blocking app startup.
    """
    time.sleep(2)  # Allow peer services to start.

    for peer_id, peer_url in get_peers():
        try:
            resp = requests.get(f"{peer_url}/leader", timeout=ELECTION_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                lid = data.get("leader_id")
                if lid is not None:
                    handle_coordinator_message(
                        lid, data.get("leader_url", peer_url)
                    )
                    logger.info(
                        "Node %d: discovered existing leader %d on startup",
                        NODE_ID,
                        lid,
                    )
                    return
        except requests.RequestException:
            pass

    # No leader found — initiate election.
    logger.info("Node %d: no leader found on startup, initiating election", NODE_ID)
    start_election()


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def init_election() -> None:
    """
    Initialise the election module.

    Must be called once on application startup. Spawns background threads
    for heartbeat sending, monitoring, and initial leader discovery.
    """
    global _running, _last_heartbeat
    _last_heartbeat = time.time()
    _running = True

    threading.Thread(
        target=_leader_heartbeat_loop, daemon=True, name="hb-sender"
    ).start()
    threading.Thread(
        target=_heartbeat_monitor_loop, daemon=True, name="hb-monitor"
    ).start()
    threading.Thread(
        target=_startup_election_check, daemon=True, name="startup-check"
    ).start()

    logger.info("Node %d: election module initialised", NODE_ID)


def shutdown() -> None:
    """Stop all background threads."""
    global _running
    _running = False
    logger.info("Node %d: election module shutting down", NODE_ID)
