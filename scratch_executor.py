import asyncio
from src.controller.executor import execute_idjc
from shared.schemas import InsightState, QueryIntent, AgencyName
from src.reasoning.cross_agency import reasoning_node

async def main():
    state = InsightState(
        messages=[{"role": "user", "content": "Show me a breakdown of all theft-related offenses in the juvenile records (IDJC)."}],
        question="Show me a breakdown of all theft-related offenses in the juvenile records (IDJC).",
        intent=QueryIntent.SINGLE_AGENCY,
        agencies=[AgencyName.IDJC],
        idhw_data={}, idjc_data={}, idoc_data={}
    )
    
    result = await execute_idjc(state)
    print("Errors:", result.get("errors", []))
    print("Traces:", result.get("execution_trace", []))
    print("IDJC breakdown:", result.get("idjc_data", {}).get("statistics", {}).get("offense_breakdown", {}))

if __name__ == "__main__":
    asyncio.run(main())
