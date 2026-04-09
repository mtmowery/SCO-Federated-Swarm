import asyncio
from src.agents.idoc.db import get_offense_summary

async def main():
    try:
        res = await get_offense_summary(keyword="murder")
        print("RESULT:")
        print(res)
    except Exception as e:
        print("ERROR:", str(e))

if __name__ == "__main__":
    asyncio.run(main())
