import asyncio
from src.controller.executor import execute_idoc
from shared.schemas import InsightState, QueryIntent, AgencyName
from src.reasoning.cross_agency import reasoning_node
from src.controller.answer import synthesize_answer

async def main():
    state = InsightState(
        messages=[{"role": "user", "content": "How many people in IDOC are in for murder? List the different types."}],
        question="How many people in IDOC are in for murder? List the different types.",
        intent=QueryIntent.SINGLE_AGENCY,
        agencies=[AgencyName.IDOC],
        idhw_data={}, idjc_data={}, idoc_data={},
        errors=[], execution_trace=[], sources=[], planner={}
    )
    
    result = await execute_idoc(state)
    state.update(result)
    
    node_res = await reasoning_node(state)
    state["reasoning_result"] = node_res.get("reasoning_result", {})
    
    try:
        ans = await synthesize_answer(state)
        print("====== LLM ANSWER ======")
        print(ans.get("answer"))
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
