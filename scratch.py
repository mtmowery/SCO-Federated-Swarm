import asyncio
from src.memory.graph_memory import GraphMemory

async def main():
    graph = GraphMemory()
    await graph.connect()
    
    query = """
    MATCH (idhw:Agency {agency_id: 'IDHW'}), (idoc:Agency {agency_id: 'IDOC'})
    MATCH (idhw)-[:IN_AGENCY]->(child:Person)<-[:PARENT_OF]-(parent:Person)<-[:IN_AGENCY]-(idoc)
    MATCH (idhw)-[:IN_AGENCY]->(parent)
    RETURN count(DISTINCT child) AS count
    """
    
    async with graph.driver.session(database=graph._database) as session:
        result = await session.run(query)
        record = await result.single()
        print(record.get('count'))

    await graph.close()

if __name__ == "__main__":
    asyncio.run(main())
