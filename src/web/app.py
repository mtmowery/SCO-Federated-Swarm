#!/usr/bin/env python3
"""
Flask Frontend Web App for Idaho Federated AI Swarm.
Bridges a modern web UI to the LangGraph Controller execution engine.

Port: 5056
"""

import os
import sys
import uuid
import logging
import asyncio
from flask import Flask, request, jsonify, render_template, send_from_directory

# Configure path so we can import from src/
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from controller.graph import run_query
from shared.logging_config import setup_logging

app = Flask(__name__)

# Basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Very basic dictionary to remember active threads (title mapping)
# Note: actual context memory is stored in LangGraph's MemorySaver.
# This dict is purely to visualize history list in UI for this mock-up.
ACTIVE_THREADS = {}
ACTIVE_TASKS = {}
ACTIVE_PROGRESS = {}


@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/chat", methods=["POST"])
async def api_chat():
    """Receive chat message and route to the LangGraph execution engine."""
    data = request.json
    question = data.get("message")
    thread_id = data.get("thread_id")
    
    if not question:
        return jsonify({"error": "Message is required"}), 400

    if not thread_id:
        # Generate new conversation thread if none provided
        thread_id = str(uuid.uuid4())
        ACTIVE_THREADS[thread_id] = {
            "title": f"{(question[:57] + '...') if len(question) > 57 else question}",
            "created_at": "Just now",
            "messages": []
        }
    
    if thread_id not in ACTIVE_THREADS:
        ACTIVE_THREADS[thread_id] = {"title": f"{(question[:57] + '...') if len(question) > 57 else question}", "messages": []}

    logger.info(f"Processing query for thread {thread_id}: {question}")
    
    # Build context from previous messages
    context = ""
    messages = ACTIVE_THREADS[thread_id]["messages"]
    if len(messages) > 0:
        context = "Previous Conversation History:\n"
        # Only include the last 4 messages for context window efficiency
        for msg in messages[-4:]:
            role = "User" if msg["role"] == "user" else "System"
            content = msg.get("content", "")
            context += f"{role}: {content}\n"
        context += "\nCurrent Question:\n"
        
    # Store user message
    messages.append({"role": "user", "content": question})

    # Prepare query with context injection
    orchestrator_query = context + question if context else question

    # Execute Swarm
    task = asyncio.current_task()
    if task:
        ACTIVE_TASKS[thread_id] = task

    ACTIVE_PROGRESS[thread_id] = {
        "status": "Initializing query resolution...",
        "progress": 5,
        "messages": [],
    }

    # State update map to estimate progress
    NODE_PROGRESS = {
        "intent": 15,
        "planner": 30,
        "router": 40,
        "execute_idhw": 60,
        "extract_parent_ids": 65,
        "execute_idjc": 80,
        "execute_idoc": 80,
        "reasoning": 90,
        "answer": 98,
    }

    def on_progress(node_name, state_update):
        prog_info = ACTIVE_PROGRESS.get(thread_id, {"progress": 10, "messages": []})
        new_prog = NODE_PROGRESS.get(node_name)
        if new_prog:
            prog_info["progress"] = max(prog_info.get("progress", 0), new_prog)
        
        traces = state_update.get("execution_trace", [])
        if isinstance(traces, list):
            for t in traces:
                if t not in prog_info["messages"]:
                    prog_info["messages"].append(t)
                prog_info["status"] = t
        ACTIVE_PROGRESS[thread_id] = prog_info

    try:
        result = await run_query(orchestrator_query, thread_id=thread_id, progress_callback=on_progress)
        
        answer = result.get("answer", "No answer could be generated.")
        sources = result.get("sources", [])
        
        # Store AI response
        ACTIVE_THREADS[thread_id]["messages"].append({"role": "assistant", "content": answer, "sources": sources})
        
        return jsonify({
            "thread_id": thread_id,
            "answer": answer,
            "sources": sources,
            "confidence": result.get("confidence", 0.0),
            "errors": result.get("errors", []),
            "execution_trace": result.get("execution_trace", [])
        })

    except asyncio.CancelledError:
        logger.info(f"Task for thread {thread_id} was cancelled.")
        return jsonify({"error": "Search stopped by user", "cancelled": True}), 499
    except Exception as e:
        logger.error(f"Error processing chat: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        ACTIVE_TASKS.pop(thread_id, None)
        ACTIVE_PROGRESS.pop(thread_id, None)


@app.route("/api/progress/<thread_id>", methods=["GET"])
def api_progress(thread_id):
    """Return live execution progress status for a specific thread."""
    prog = ACTIVE_PROGRESS.get(thread_id, {"progress": 0, "status": "Starting...", "messages": []})
    return jsonify(prog)


@app.route("/api/chat/<thread_id>/title", methods=["PUT"])
def api_rename_thread(thread_id):
    """Rename a conversation thread."""
    data = request.json
    new_title = data.get("title")
    if not new_title:
        return jsonify({"error": "Title required"}), 400
        
    if thread_id in ACTIVE_THREADS:
        ACTIVE_THREADS[thread_id]["title"] = new_title
        return jsonify({"success": True})
    return jsonify({"error": "Thread not found"}), 404

@app.route("/api/history", methods=["GET"])
def api_history():
    """Return list of active threads metadata (strictly for UI sidebar)."""
    return jsonify({
        # Returns id & title
        tid: info["title"] for tid, info in ACTIVE_THREADS.items()
    })

@app.route("/api/history/<thread_id>", methods=["GET"])
def api_history_thread(thread_id):
    """Fetch messages for a specific thread."""
    if thread_id in ACTIVE_THREADS:
        return jsonify(ACTIVE_THREADS[thread_id]["messages"])
    return jsonify({"error": "Thread not found"}), 404


@app.route("/api/cancel", methods=["POST"])
def api_cancel():
    """Cancel an ongoing query by thread ID."""
    data = request.json or {}
    thread_id = data.get("thread_id")
    if not thread_id:
        return jsonify({"error": "Thread ID required"}), 400
        
    task = ACTIVE_TASKS.get(thread_id)
    if task and not task.done():
        task.cancel()
        logger.info(f"Initiated cancellation for thread {thread_id}")
        return jsonify({"status": "cancelled", "thread_id": thread_id})
    else:
        return jsonify({"status": "no_active_task", "thread_id": thread_id})


if __name__ == "__main__":
    setup_logging()
    app.run(host="0.0.0.0", port=5056, debug=False)
