import asyncio
from src.controller.planner import _llm_plan

async def main():
    q = "give a list of the top 10 idjc individuals with the most offenses"
    intent, plan, agencies = await _llm_plan(q)
    print("Intent:", intent)
    print("Plan:", plan)
    print("Agencies:", agencies)

if __name__ == "__main__":
    import os
    os.environ["PYTHONPATH"] = "src"
    asyncio.run(main())
