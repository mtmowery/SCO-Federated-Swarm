import asyncio
from shared.schemas import InsightState, QueryIntent, AgencyName
from src.controller.executor import execute_idhw, execute_idoc
from src.reasoning.cross_agency import CrossAgencyReasoner

async def main():
    state = InsightState(
        messages=[{"role": "user", "content": "how many kids in foster care have parents in idoc that also were in foster care"}],
        question="how many kids in foster care have parents in idoc that also were in foster care",
        intent=QueryIntent.CROSS_AGENCY,
        agencies=[AgencyName.IDHW, AgencyName.IDOC],
        idhw_data={}, idjc_data={}, idoc_data={},
        errors=[], execution_trace=[], sources=[], planner={}
    )
    
    res_idhw = await execute_idhw(state)
    state.update(res_idhw)
    res_idoc = await execute_idoc(state)
    state.update(res_idoc)
    
    reasoner = CrossAgencyReasoner()
    reasoner.build_family_graph(state["idhw_data"])
    reasoner.add_incarceration_data(state["idoc_data"])

    foster_ids = {e.target_id for e in reasoner.graph.get_neighbors("IDHW", "IN_AGENCY")}
    idoc_ids = {e.target_id for e in reasoner.graph.get_neighbors("IDOC", "IN_AGENCY")}

    parents_in_idoc_and_foster = [p for p in reasoner.graph.nodes if p in idoc_ids and p in foster_ids]
    
    for p in parents_in_idoc_and_foster:
        print(f"Edges for {p}:")
        for e in reasoner.graph.edges:
            if e.target_id == p or e.source_id == p:
                print("  ", e.source_id, "->", e.target_id, e.relationship_type)

if __name__ == "__main__":
    asyncio.run(main())
