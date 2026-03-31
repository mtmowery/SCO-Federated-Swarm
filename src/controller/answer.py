"""
Answer synthesis node for generating natural language responses.

Transforms structured query results into conversational answers with:
- Natural language generation via Ollama LLM
- Data provenance information
- Confidence scoring
- Graceful fallback to template-based answers
"""

import logging
from typing import Any

from shared.schemas import InsightState, Provenance, AgencyName, QueryType
from shared.config import settings
from datetime import datetime

try:
    from langchain_ollama import OllamaLLM as Ollama
except ImportError:
    from langchain_community.llms.ollama import Ollama

logger = logging.getLogger(__name__)


async def synthesize_answer(state: InsightState) -> dict:
    """
    Generate natural language answer from reasoning results.

    Returns partial state dict for LangGraph reducer compatibility.
    """
    reasoning_result = state.get("reasoning_result", {})
    sources = state.get("sources", [])
    intent = state.get("intent")
    question = state.get("question", "")
    traces = []

    if not reasoning_result and not sources:
        return {
            "answer": "Unable to find relevant information to answer your question.",
            "confidence": 0.0,
            "execution_trace": [],
        }

    # Try LLM-based synthesis
    try:
        answer = await _llm_synthesize_answer(
            question=question,
            reasoning_result=reasoning_result,
            sources=sources,
            intent=intent,
        )
        confidence = _calculate_confidence(reasoning_result, sources)
    except Exception as e:
        logger.warning(f"LLM synthesis failed: {e}, using template-based answer")
        answer = _template_based_answer(reasoning_result, sources, intent)
        confidence = _calculate_confidence(reasoning_result, sources)

    if sources:
        traces.append(f"Answer synthesized from {', '.join(sources)}")

    return {
        "answer": answer,
        "confidence": confidence,
        "execution_trace": traces,
    }


async def _llm_synthesize_answer(
    question: str,
    reasoning_result: dict[str, Any],
    sources: list[str],
    intent: Any,
) -> str:
    """
    Use LLM to generate natural language answer.

    Args:
        question: Original question
        reasoning_result: Structured reasoning results
        sources: Agencies that contributed data
        intent: Query intent

    Returns:
        Natural language answer

    Raises:
        Exception: If LLM invocation fails
    """
    llm = Ollama(
        base_url=settings.ollama.base_url,
        model=settings.ollama.default_model,
        temperature=settings.ollama.temperature,
        top_p=settings.ollama.top_p,
    )

    # Format reasoning result for prompt
    result_str = _format_reasoning_result(reasoning_result)
    sources_str = ", ".join(sources) if sources else "unknown sources"

    prompt = f"""You are a helpful assistant synthesizing results from a cross-agency federated AI system.

Original Question: {question}

Data from agencies ({sources_str}):
{result_str}

Provide a clear, concise, natural language answer to the original question. Focus on answering the specific question asked.
Be conversational but professional. Include relevant details from the data.
If there are limitations or gaps in the data, mention them briefly."""

    try:
        response = llm.invoke(prompt)
        return response.strip()
    except Exception as e:
        raise Exception(f"LLM invocation failed: {e}")


def _format_reasoning_result(reasoning_result: dict[str, Any]) -> str:
    """
    Format reasoning result as readable text.

    Args:
        reasoning_result: Structured reasoning results

    Returns:
        Formatted text
    """
    if not reasoning_result:
        return "No data found."

    lines = []

    # Format IDHW data
    idhw_data = reasoning_result.get("idhw_data", {})
    if idhw_data:
        lines.append("From IDHW (Foster Care & Family Services):")
        if idhw_data.get("child_records"):
            lines.append(f"  - Found {len(idhw_data['child_records'])} child records")
        if idhw_data.get("family_relationships"):
            lines.append(f"  - Found {len(idhw_data['family_relationships'])} family relationships")

    # Format IDJC data
    idjc_data = reasoning_result.get("idjc_data", {})
    if idjc_data:
        lines.append("From IDJC (Juvenile Corrections):")
        if idjc_data.get("commitments"):
            lines.append(f"  - Found {len(idjc_data['commitments'])} commitment records")

    # Format IDOC data
    idoc_data = reasoning_result.get("idoc_data", {})
    if idoc_data:
        lines.append("From IDOC (Adult Corrections):")
        if idoc_data.get("inmates"):
            lines.append(f"  - Found {len(idoc_data['inmates'])} inmate records")

    # Format cross-agency matches
    identity_matches = reasoning_result.get("identity_matches", {})
    if identity_matches:
        lines.append("Cross-Agency Identity Matches:")
        match_count = len(identity_matches.get("matches", []))
        lines.append(f"  - {match_count} identities matched across agencies")

    return "\n".join(lines) if lines else "No structured data found."


def _template_based_answer(
    reasoning_result: dict[str, Any],
    sources: list[str],
    intent: Any,
) -> str:
    """
    Generate answer using template approach.

    Fallback when LLM synthesis fails.

    Args:
        reasoning_result: Structured reasoning results
        sources: Agencies that contributed data
        intent: Query intent

    Returns:
        Template-based answer
    """
    if not reasoning_result and not sources:
        return "No information was found to answer your question."

    source_str = ", ".join(sources) if sources else "the available data"

    # Count results
    idhw_count = 0
    idjc_count = 0
    idoc_count = 0

    idhw_data = reasoning_result.get("idhw_data", {})
    idhw_count = (
        len(idhw_data.get("child_records", []))
        + len(idhw_data.get("family_relationships", []))
    )

    idjc_data = reasoning_result.get("idjc_data", {})
    idjc_count = len(idjc_data.get("commitments", []))

    idoc_data = reasoning_result.get("idoc_data", {})
    idoc_count = len(idoc_data.get("inmates", []))

    total_results = idhw_count + idjc_count + idoc_count

    if total_results == 0:
        return f"No matching records were found in {source_str}."

    # Build answer based on counts
    answer_parts = [f"Based on data from {source_str}:"]

    if idhw_count > 0:
        answer_parts.append(f"- IDHW records: {idhw_count} result(s)")
    if idjc_count > 0:
        answer_parts.append(f"- IDJC records: {idjc_count} result(s)")
    if idoc_count > 0:
        answer_parts.append(f"- IDOC records: {idoc_count} result(s)")

    answer_parts.append(f"\nTotal: {total_results} records found matching your query.")

    return "\n".join(answer_parts)


def _calculate_confidence(
    reasoning_result: dict[str, Any],
    sources: list[str],
) -> float:
    """
    Calculate confidence score for answer.

    Based on number of sources and amount of data returned.

    Args:
        reasoning_result: Structured reasoning results
        sources: Agencies that contributed data

    Returns:
        Confidence score (0.0 to 1.0)
    """
    if not sources:
        return 0.0

    if not reasoning_result:
        return 0.2 * len(sources) / 3

    # Count data points
    data_count = 0

    idhw_data = reasoning_result.get("idhw_data", {})
    data_count += len(idhw_data.get("child_records", []))
    data_count += len(idhw_data.get("family_relationships", []))

    idjc_data = reasoning_result.get("idjc_data", {})
    data_count += len(idjc_data.get("commitments", []))

    idoc_data = reasoning_result.get("idoc_data", {})
    data_count += len(idoc_data.get("inmates", []))

    identity_matches = reasoning_result.get("identity_matches", {})
    data_count += len(identity_matches.get("matches", []))

    # Base confidence on sources and data volume
    # 1 source, some data: 0.7
    # 2 sources, some data: 0.85
    # 3 sources, some data: 0.95
    source_confidence = min(0.7 + (len(sources) - 1) * 0.15, 0.95)

    # Adjust down if no data
    if data_count == 0:
        return source_confidence * 0.3

    # Adjust up if we have cross-agency matches
    if identity_matches:
        source_confidence = min(source_confidence + 0.05, 1.0)

    return source_confidence
