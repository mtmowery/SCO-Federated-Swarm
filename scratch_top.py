import asyncio
from shared.schemas import InsightState, QueryIntent, AgencyName
from src.controller.executor import execute_idjc
from src.reasoning.cross_agency import reasoning_node

async def main():
    state = InsightState(
        messages=[{"role": "user", "content": "give a list of the top 10 idjc individuals with the most offenses"}],
        question="give a list of the top 10 idjc individuals with the most offenses",
        intent=QueryIntent.STATISTICS,
        agencies=[AgencyName.IDJC],
        idhw_data={}, idjc_data={}, idoc_data={},
        errors=[], execution_trace=[], sources=[], planner={}, plan=[]
    )
    
    res_idjc = await execute_idjc(state)
    state.update(res_idjc)
    print("idjc data stats keys:", state.get("idjc_data", {}).get("statistics", {}).keys())
    
    res_reason = await reasoning_node(state)
    print("Reasoning res:", res_reason)

if __name__ == "__main__":
    asyncio.run(main())
