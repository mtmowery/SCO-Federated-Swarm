import asyncio
from src.agents.idjc.db import get_offense_summary

async def main():
    try:
        res = await get_offense_summary("theft")
        print(res)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
