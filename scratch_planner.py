import asyncio
from src.controller.planner import plan_query
from shared.schemas import InsightState

async def main():
    state = InsightState(
        messages=[{"role": "user", "content": "how many people in idjc were also in idoc?"}],
        question="how many people in idjc were also in idoc?"
    )
    res = await plan_query(state)
    print("INTENT:", res["intent"])
    print("AGENCIES:", res["agencies"])

asyncio.run(main())
