"""
Planner agent node for query analysis and execution planning.

Analyzes natural language questions and produces:
- Intent classification (cross_agency, single_agency, statistics, lookup)
- Execution plan (list of steps)
- Required agencies
"""

import logging
from typing import Any

from shared.schemas import InsightState, QueryIntent, AgencyName
from shared.config import settings

try:
    from langchain_ollama import OllamaLLM as Ollama
except ImportError:
    from langchain_community.llms.ollama import Ollama

logger = logging.getLogger(__name__)


class PlanningFailure(Exception):
    """Raised when planning fails."""

    pass


async def plan_query(state: InsightState) -> dict:
    """
    Analyze question and produce execution plan.

    Uses LLM to classify intent and determine required agencies.
    Falls back to keyword-based routing if LLM fails.

    Returns partial state dict for LangGraph reducer compatibility.
    """
    question = state.get("question", "")
    errors = []

    if not question:
        errors.append("No question provided")
        return {"errors": errors, "execution_trace": []}

    # Try LLM-based planning
    try:
        intent, plan, agencies = await _llm_plan(question)
    except Exception as e:
        logger.warning(f"LLM planning failed: {e}, falling back to keyword routing")
        intent, agencies = _keyword_based_routing(question)
        plan = _build_default_plan(intent, agencies)

    return {
        "intent": intent,
        "plan": plan,
        "agencies": agencies,
        "errors": errors,
        "execution_trace": [
            f"Planned query: intent={intent.value}, agencies={[a.value for a in agencies]}"
        ],
    }


async def _llm_plan(question: str) -> tuple[QueryIntent, list[str], list[AgencyName]]:
    """
    Use LLM to analyze question and produce plan.

    Args:
        question: Natural language question

    Returns:
        Tuple of (intent, plan, agencies)

    Raises:
        PlanningFailure: If LLM analysis fails
    """
    llm = Ollama(
        base_url=settings.ollama.base_url,
        model=settings.ollama.default_model,
        temperature=settings.ollama.temperature,
        top_p=settings.ollama.top_p,
    )

    prompt = f"""You are a query planner for an Idaho federated AI swarm. Analyze this question and provide:
1. Intent: one of [cross_agency, single_agency, statistics, lookup]
2. Execution plan: step-by-step actions to answer the question
3. Required agencies: list from [idhw, idjc, idoc]

Question: {question}

Respond in this exact format:
INTENT: <intent>
PLAN:
- <step 1>
- <step 2>
- <step 3>
AGENCIES: <comma-separated list>

Be concise. Think about what agencies would have relevant data."""

    try:
        response = llm.invoke(prompt)
        return _parse_plan_response(response)
    except Exception as e:
        raise PlanningFailure(f"LLM invocation failed: {e}")


def _parse_plan_response(response: str) -> tuple[QueryIntent, list[str], list[AgencyName]]:
    """
    Parse LLM response into structured plan.

    Args:
        response: LLM response text

    Returns:
        Tuple of (intent, plan, agencies)

    Raises:
        PlanningFailure: If response cannot be parsed
    """
    try:
        lines = response.strip().split("\n")

        # Extract intent
        intent_line = next(
            (line for line in lines if line.startswith("INTENT:")), None
        )
        if not intent_line:
            raise PlanningFailure("No INTENT line in response")

        intent_str = intent_line.replace("INTENT:", "").strip().lower()
        intent = QueryIntent(intent_str)

        # Extract plan
        plan = []
        in_plan = False
        for line in lines:
            if line.startswith("PLAN:"):
                in_plan = True
                continue
            if line.startswith("AGENCIES:"):
                in_plan = False
            if in_plan and line.startswith("- "):
                plan.append(line[2:].strip())

        if not plan:
            plan = ["Query agency data"]

        # Extract agencies
        agencies_line = next(
            (line for line in lines if line.startswith("AGENCIES:")), None
        )
        if not agencies_line:
            raise PlanningFailure("No AGENCIES line in response")

        agencies_str = agencies_line.replace("AGENCIES:", "").strip().lower()
        agencies = []
        for agency_name in agencies_str.split(","):
            agency_name = agency_name.strip()
            if agency_name in [a.value for a in AgencyName]:
                agencies.append(AgencyName(agency_name))

        if not agencies:
            raise PlanningFailure("No valid agencies extracted")

        return intent, plan, agencies

    except (ValueError, StopIteration, IndexError) as e:
        raise PlanningFailure(f"Failed to parse plan response: {e}")


def _keyword_based_routing(question: str) -> tuple[QueryIntent, list[AgencyName]]:
    """
    Fall back to keyword-based intent classification.

    Uses simple keyword matching to route to appropriate agencies.

    Args:
        question: Natural language question

    Returns:
        Tuple of (intent, agencies)
    """
    question_lower = question.lower()

    # Define keyword patterns for each agency
    idhw_keywords = ["foster", "child", "welfare", "family", "parent", "guardian", "caregiver"]
    idjc_keywords = ["juvenile", "youth", "detention", "delinquent", "minor", "teen"]
    idoc_keywords = ["prison", "incarcerat", "offender", "inmate", "sentence", "parole", "felon"]

    agencies = []

    # Check for IDHW keywords
    if any(kw in question_lower for kw in idhw_keywords):
        agencies.append(AgencyName.IDHW)

    # Check for IDJC keywords
    if any(kw in question_lower for kw in idjc_keywords):
        agencies.append(AgencyName.IDJC)

    # Check for IDOC keywords
    if any(kw in question_lower for kw in idoc_keywords):
        agencies.append(AgencyName.IDOC)

    # Default to all agencies if no keywords match
    if not agencies:
        agencies = list(AgencyName)

    # Determine intent based on question patterns
    if "relationship" in question_lower or "family" in question_lower:
        intent = QueryIntent.RELATIONSHIP
    elif any(kw in question_lower for kw in ["count", "how many", "total", "statistics"]):
        intent = QueryIntent.STATISTICS
    elif len(agencies) > 1:
        intent = QueryIntent.CROSS_AGENCY
    else:
        intent = QueryIntent.LOOKUP

    return intent, agencies


def _build_default_plan(intent: QueryIntent, agencies: list[AgencyName]) -> list[str]:
    """
    Build default execution plan based on intent and agencies.

    Args:
        intent: Query intent
        agencies: Required agencies

    Returns:
        List of execution steps
    """
    plan = []

    if intent == QueryIntent.CROSS_AGENCY:
        if AgencyName.IDHW in agencies:
            plan.append("Query IDHW for family relationships and child information")
        if AgencyName.IDJC in agencies:
            plan.append("Query IDJC for juvenile justice records")
        if AgencyName.IDOC in agencies:
            plan.append("Query IDOC for adult incarceration records")
        plan.append("Cross-reference results via identity matching")

    elif intent == QueryIntent.RELATIONSHIP:
        plan.append("Query IDHW for family relationships")
        plan.append("Extract parent IDs from relationships")
        plan.append("Check other agencies for records")

    elif intent == QueryIntent.STATISTICS:
        for agency in agencies:
            plan.append(f"Aggregate statistics from {agency.value}")
        plan.append("Combine and summarize results")

    else:  # LOOKUP
        for agency in agencies:
            plan.append(f"Look up records in {agency.value}")
        plan.append("Return combined results")

    return plan
