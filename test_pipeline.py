import asyncio
from src.controller.executor import execute_idoc
from shared.schemas import InsightState, QueryIntent, AgencyName
from src.reasoning.cross_agency import reasoning_node
from src.controller.answer import synthesize_answer

async def main():
    state = InsightState(
        messages=[{"role": "user", "content": "how many people in IDOC are in for murder? list the types"}],
        question="how many people in IDOC are in for murder? list the types",
        intent=QueryIntent.SINGLE_AGENCY,
        agencies=[AgencyName.IDOC],
        idhw_data={}, idjc_data={}, idoc_data={},
        errors=[], execution_trace=[], sources=[]
    )
    
    # Run IDOC execution
    idoc_result = await execute_idoc(state)
    state.update(idoc_result)
    
    print("ERRORS:", state.get("errors", []))
    print("IDOC DATA STATISTICS:", state.get("idoc_data", {}).get("statistics", {}).keys())
    
    # Run reasoning
    reason_result = await reasoning_node(state)
    state["reasoning_result"] = reason_result
    print("REASONING TYPE:", reason_result.get("query_type"))
    print("REASONING BREAKDOWN:", reason_result.get("breakdown"))
    
    # Run answer
    # Note: OLLAMA relies on a base url. 
    # Just print the formatted string to bypass LLM logic.
    from src.controller.answer import _format_reasoning_result
    formatted = _format_reasoning_result(reason_result)
    print("Formatted Result String for LLM Prompt:\n", formatted)

if __name__ == "__main__":
    asyncio.run(main())
