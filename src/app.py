from datetime import datetime, timezone

from fastapi import Depends, FastAPI, HTTPException, Response
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.database import Base, engine, get_db
from src.election import (
    get_leader_info,
    handle_coordinator_message,
    handle_election_message,
    handle_heartbeat,
    init_election,
    shutdown,
)
from src.models import Node
from src.schemas import NodeCreate, NodeResponse, NodeUpdate

# checkfirst=True avoids race conditions when multiple nodes
# try to create the same tables concurrently on startup.
Base.metadata.create_all(bind=engine, checkfirst=True)
app = FastAPI()


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


@app.on_event("startup")
def on_startup():
    init_election()


@app.on_event("shutdown")
def on_shutdown():
    shutdown()


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        db_status = "disconnected"
    count = db.query(Node).filter(Node.status == "active").count()
    return {"status": "ok", "db": db_status, "nodes_count": count}


# ---------------------------------------------------------------------------
# Node Registry CRUD
# ---------------------------------------------------------------------------


@app.post("/api/nodes", response_model=NodeResponse, status_code=201)
def register_node(node: NodeCreate, db: Session = Depends(get_db)):
    existing = db.query(Node).filter(Node.name == node.name).first()
    if existing:
        raise HTTPException(status_code=409, detail="Node already exists")
    db_node = Node(name=node.name, host=node.host, port=node.port)
    db.add(db_node)
    db.commit()
    db.refresh(db_node)
    return db_node


@app.get("/api/nodes", response_model=list[NodeResponse])
def list_nodes(db: Session = Depends(get_db)):
    return db.query(Node).all()


@app.get("/api/nodes/{name}", response_model=NodeResponse)
def get_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node


@app.put("/api/nodes/{name}", response_model=NodeResponse)
def update_node(name: str, update: NodeUpdate, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    if update.host is not None:
        node.host = update.host
    if update.port is not None:
        node.port = update.port
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(node)
    return node


@app.delete("/api/nodes/{name}", status_code=204)
def delete_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    node.status = "inactive"
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Election endpoints
# ---------------------------------------------------------------------------


@app.post("/election")
def election_endpoint(body: dict):
    """
    Receive an ELECTION message from a peer.

    Request body: {"sender_id": int}
    Response: {"response": "alive" | "ignored", "node_id": int}
    """
    sender_id = body.get("sender_id")
    if sender_id is None:
        raise HTTPException(status_code=400, detail="Missing sender_id")
    return handle_election_message(sender_id)


@app.post("/coordinator")
def coordinator_endpoint(body: dict):
    """
    Receive a COORDINATOR (leader announcement) message.

    Request body: {"leader_id": int, "leader_url": str}
    """
    leader_id = body.get("leader_id")
    leader_url = body.get("leader_url", "")
    if leader_id is None:
        raise HTTPException(status_code=400, detail="Missing leader_id")
    handle_coordinator_message(leader_id, leader_url)
    return {"status": "ok"}


@app.post("/heartbeat")
def heartbeat_endpoint(body: dict):
    """
    Receive a heartbeat from the leader.

    Request body: {"leader_id": int}
    """
    leader_id = body.get("leader_id")
    if leader_id is None:
        raise HTTPException(status_code=400, detail="Missing leader_id")
    handle_heartbeat(leader_id)
    return {"status": "ok"}


@app.get("/leader")
def leader_endpoint():
    """Return the current leader information."""
    return get_leader_info()
