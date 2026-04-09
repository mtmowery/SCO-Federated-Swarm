import asyncio
from shared.schemas import InsightState, QueryIntent, AgencyName
from src.controller.executor import execute_idjc
from src.controller.answer import _format_reasoning_result

async def main():
    state = InsightState(
        messages=[{"role": "user", "content": "give a list of the top 10 idjc individuals with the most offenses"}],
        question="give a list of the top 10 idjc individuals with the most offenses",
        intent=QueryIntent.STATISTICS,
        agencies=[AgencyName.IDJC],
        idhw_data={}, idjc_data={}, idoc_data={},
        errors=[], execution_trace=[], sources=[], planner={}
    )
    
    res_idjc = await execute_idjc(state)
    state.update(res_idjc)
    
    # We simulate reasoning node grabbing statistics from idjc_data
    stats = state["idjc_data"].get("statistics", {})
    breakdown = stats.get("by_status", {})
    if "top_offenders" in stats:
        breakdown = {"top_offenders": stats["top_offenders"]}
        
    reasoning_result = {
        "query_type": "single_agency_statistics",
        "count": stats.get("total_people", 0),
        "total_records": stats.get("total_records", 0),
        "agency": "idjc",
        "breakdown": breakdown
    }
    
    formatted = _format_reasoning_result(reasoning_result)
    print("Formatted result:\n")
    print(formatted)

if __name__ == "__main__":
    asyncio.run(main())
