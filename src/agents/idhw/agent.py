"""IDHW specialist agent for foster care data analysis.

Provides LLM-powered reasoning over IDHW foster care data using MCP tools
and Ollama for language understanding.
"""

import logging
import json
from typing import Any, Optional
from datetime import datetime
from uuid import uuid4

import httpx

from shared.schemas import AgentResponse, Provenance, ResponseStatus, QueryType, AgencyName
from shared.config import settings
from . import db

logger = logging.getLogger(__name__)

# System prompt for IDHW agent
IDHW_SYSTEM_PROMPT = """You are an expert analyst for the Idaho Department of Health and Welfare (IDHW) foster care system.

Your role is to help answer questions about foster care data including:
- Children in foster care and their family relationships
- Parents (mothers and fathers) and their children
- Foster care dates, legal events (TPR - Termination of Parental Rights), deaths
- Aggregations and statistics about the foster care population

You have access to tools that allow you to:
- Look up individual children, mothers, and fathers
- Find family relationships between children and parents
- Search for people by various criteria (name, gender, agency ID, etc.)
- Analyze aggregate statistics about the foster care system

When answering questions:
1. Use exact insight_ids when referring to specific individuals
2. Always cite which records you consulted
3. Be precise about dates and legal events
4. Acknowledge limitations in the data (missing values, errors)
5. Distinguish between children in care vs. children who have exited care
6. Provide confidence levels for your conclusions

Remember that foster care data involves vulnerable populations. Be respectful and accurate in your analysis."""


class IDHWAgent:
    """Specialist agent for IDHW foster care data.

    Combines MCP tools with LLM reasoning for intelligent data analysis.
    """

    def __init__(self, mcp_base_url: Optional[str] = None):
        """Initialize IDHW agent.

        Args:
            mcp_base_url: Base URL for MCP server (default from config)
        """
        self.mcp_base_url = mcp_base_url or f"http://{settings.mcp.idhw_host}:{settings.mcp.idhw_port}"
        self.http_client = httpx.AsyncClient(timeout=settings.mcp.timeout)
        self.ollama_base_url = settings.ollama.base_url
        self.ollama_model = settings.ollama.default_model
        self.logger = logging.getLogger(__name__)

    async def query(self, question: str) -> AgentResponse:
        """Process a question about IDHW foster care data.

        Uses MCP tools to gather data and Ollama for reasoning.

        Args:
            question: Natural language question about foster care data

        Returns:
            AgentResponse with results and provenance
        """
        request_id = uuid4()

        try:
            # Step 1: Analyze the question to determine which tools to use
            intent = await self._classify_intent(question)
            self.logger.info(f"Request {request_id}: Classified intent as {intent}")

            # Step 2: Gather relevant data using tools
            tool_results = await self._execute_tools(intent, question)
            self.logger.info(f"Request {request_id}: Executed tools, got {len(tool_results)} results")

            # Step 3: Reason over the data using Ollama
            answer = await self._reason_over_data(question, tool_results)
            self.logger.info(f"Request {request_id}: Generated answer")

            # Step 4: Format response
            return AgentResponse(
                request_id=request_id,
                status=ResponseStatus.SUCCESS,
                data={
                    "question": question,
                    "answer": answer,
                    "intent": intent,
                    "tool_results_count": len(tool_results),
                },
                provenance=[
                    Provenance(
                        agency=AgencyName.IDHW,
                        query_type=QueryType.LOOKUP,
                        timestamp=datetime.utcnow(),
                    )
                ],
                confidence=0.9,
            )

        except Exception as e:
            self.logger.error(f"Request {request_id}: Query failed - {str(e)}")
            return AgentResponse(
                request_id=request_id,
                status=ResponseStatus.ERROR,
                error_message=f"Query processing failed: {str(e)}",
                confidence=0.0,
            )

    async def get_child_parent_map(self) -> dict[str, Any]:
        """Get mapping of children to parents.

        Returns:
            Dictionary with child_insight_id keys and parent info values
        """
        parent_map = await db.get_parent_map()
        # Reverse the map to show children -> parents
        child_map: dict[str, dict[str, Any]] = {}

        # Get all children
        children = await db.get_all_children()
        for child in children:
            child_id = child["insight_id"]
            child_map[child_id] = {
                "mother_insight_id": child["mother_insight_id"],
                "father_insight_id": child["father_insight_id"],
            }

        return child_map

    async def get_foster_parent_ids(self) -> list[str]:
        """Get insight_ids of all parents with children in foster care.

        Returns:
            List of parent insight_ids
        """
        parent_map = await db.get_parent_map()
        # Get foster children
        foster_children = await db.get_foster_children()
        foster_child_ids = {child["insight_id"] for child in foster_children}

        # Filter parent_map to only parents of foster children
        foster_parents = set()
        for parent_id, children in parent_map.items():
            if any(child_id in foster_child_ids for child_id in children):
                foster_parents.add(parent_id)

        return list(foster_parents)

    async def aggregate_count(self, filters: dict[str, Any]) -> dict[str, Any]:
        """Get aggregate counts with optional filters.

        Args:
            filters: Dictionary of filter criteria

        Returns:
            Aggregation results
        """
        people = await db.search_people(filters)
        return {"count": len(people), "records": people}

    async def close(self):
        """Close HTTP client."""
        await self.http_client.aclose()

    # Private helper methods

    async def _classify_intent(self, question: str) -> str:
        """Classify the intent of a question using Ollama.

        Args:
            question: User's natural language question

        Returns:
            Intent classification string
        """
        prompt = f"""Classify the intent of this question about foster care data in one word:
        - "lookup" if asking about a specific person or small set
        - "relationship" if asking about family connections
        - "aggregate" if asking for counts or statistics
        - "search" if asking to find people matching criteria

        Question: {question}

        Intent:"""

        try:
            response = await self._call_ollama(prompt)
            intent = response.strip().lower().split()[0]
            return intent
        except Exception as e:
            self.logger.warning(f"Intent classification failed: {e}, defaulting to 'search'")
            return "search"

    async def _execute_tools(self, intent: str, question: str) -> list[dict[str, Any]]:
        """Execute appropriate tools based on intent.

        Args:
            intent: Classified intent
            question: Original question

        Returns:
            List of tool execution results
        """
        results = []

        try:
            if intent == "lookup":
                # Try to extract insight_ids from question
                stats = await db.get_stats()
                results.append({"type": "stats", "data": stats})

            elif intent == "relationship":
                relationships = await db.get_family_relationships()
                parent_map = await db.get_parent_map()
                results.append({"type": "relationships", "data": relationships})
                results.append({"type": "parent_map", "data": parent_map})

            elif intent == "aggregate":
                stats = await db.get_stats()
                counts = await db.count_children_by_end_reason()
                results.append({"type": "stats", "data": stats})
                results.append({"type": "end_reasons", "data": counts})

            else:  # search
                # Get all children and search stats
                children = await db.get_all_children()
                foster_children = await db.get_foster_children()
                results.append({"type": "all_children", "count": len(children)})
                results.append({"type": "foster_children", "count": len(foster_children)})

        except Exception as e:
            self.logger.error(f"Tool execution failed: {e}")

        return results

    async def _reason_over_data(self, question: str, tool_results: list[dict[str, Any]]) -> str:
        """Use Ollama to reason over gathered data and formulate answer.

        Args:
            question: Original question
            tool_results: Results from tool executions

        Returns:
            Natural language answer
        """
        # Format tool results for the prompt
        results_text = json.dumps(tool_results, indent=2, default=str)

        prompt = f"""{IDHW_SYSTEM_PROMPT}

Based on the following data about IDHW foster care:

{results_text}

Answer this question:
{question}

Provide a clear, evidence-based answer that cites specific records and statistics."""

        try:
            answer = await self._call_ollama(prompt, max_tokens=500)
            return answer.strip()
        except Exception as e:
            self.logger.error(f"Reasoning failed: {e}")
            return "Unable to generate answer due to processing error."

    async def _call_ollama(self, prompt: str, max_tokens: int = 200) -> str:
        """Call Ollama LLM for text generation.

        Args:
            prompt: Text prompt
            max_tokens: Maximum tokens to generate

        Returns:
            Generated text response
        """
        try:
            response = await self.http_client.post(
                f"{self.ollama_base_url}/api/generate",
                json={
                    "model": self.ollama_model,
                    "prompt": prompt,
                    "stream": False,
                    "temperature": settings.ollama.temperature,
                    "top_p": settings.ollama.top_p,
                    "num_predict": max_tokens,
                },
                timeout=settings.ollama.timeout,
            )
            response.raise_for_status()
            data = response.json()
            return data.get("response", "")
        except Exception as e:
            self.logger.error(f"Ollama call failed: {e}")
            raise
