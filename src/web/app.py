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
            "title": f"{question[:30]}...",
            "created_at": "Just now",
            "messages": []
        }
    
    if thread_id not in ACTIVE_THREADS:
        ACTIVE_THREADS[thread_id] = {"title": "Restored Conversation", "messages": []}

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
    try:
        result = await run_query(orchestrator_query, thread_id=thread_id)
        
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

    except Exception as e:
        logger.error(f"Error processing chat: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


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


if __name__ == "__main__":
    setup_logging()
    app.run(host="0.0.0.0", port=5056, debug=False)
