import asyncio
from src.controller.executor import execute_idoc
from shared.schemas import InsightState, QueryIntent, AgencyName
from src.reasoning.cross_agency import reasoning_node

async def main():
    state = InsightState(
        messages=[{"role": "user", "content": "how many people in IDOC are in for murder? list the types"}],
        question="how many people in IDOC are in for murder? list the types",
        intent=QueryIntent.SINGLE_AGENCY,
        agencies=[AgencyName.IDOC],
        idhw_data={}, idjc_data={}, idoc_data={}
    )
    
    idoc_result = await execute_idoc(state)
    state.update(idoc_result)
    
    print("Traces:", state.get("execution_trace", []))
    print("IDOC Data:", state.get("idoc_data", {}).get("statistics", {}).get("offense_breakdown", {}))
    
    reason_result = await reasoning_node(state)
    print("Reasoning count:", reason_result.get("reasoning_result", {}).get("count"))
    print("Reasoning breakdown:", reason_result.get("reasoning_result", {}).get("breakdown"))

asyncio.run(main())
