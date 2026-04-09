import asyncio
from src.controller.executor import execute_idjc
from shared.schemas import InsightState, QueryIntent, AgencyName
from src.reasoning.cross_agency import reasoning_node
from src.controller.answer import _format_reasoning_result

async def main():
    state = InsightState(
        messages=[{"role": "user", "content": "Show me a breakdown of all theft-related offenses in the juvenile records (IDJC)."}],
        question="Show me a breakdown of all theft-related offenses in the juvenile records (IDJC).",
        intent=QueryIntent.SINGLE_AGENCY,
        agencies=[AgencyName.IDJC],
        idhw_data={}, idjc_data={}, idoc_data={},
        errors=[], execution_trace=[], sources=[], planner={}
    )
    
    result = await execute_idjc(state)
    state.update(result)
    
    node_res = await reasoning_node(state)
    reason_result = node_res.get("reasoning_result", {})
    
    print("Reasoning count:", reason_result.get("count"))
    print("Reasoning breakdown keys:", reason_result.get("breakdown", {}).keys())
    
    formatted = _format_reasoning_result(reason_result)
    print("\n--- FORMATTED ---")
    print(formatted)

if __name__ == "__main__":
    asyncio.run(main())
