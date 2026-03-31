"""
Agent contract definitions for Idaho Federated AI Swarm.

Defines the capabilities, data domains, and entity types for each agency agent.
Based on actual CSV schema analysis.
"""

from datetime import datetime
from .schemas import AgencyCapability, AgencyName


class AgentContract:
    """Container for agent capability contracts."""

    @staticmethod
    def get_idhw_contract() -> AgencyCapability:
        """
        Get IDHW (Idaho Department of Health and Welfare) agent contract.

        IDHW manages foster care data including:
        - Child in care details
        - Family relationships (mother, father)
        - Care episodes and outcomes
        - Termination of parental rights (TPR) events
        """
        return AgencyCapability(
            agent_id="agent-idhw-001",
            agency=AgencyName.IDHW,
            version="1.0.0",
            description="Idaho Department of Health and Welfare - Foster Care & Family Services",
            data_domain=[
                "foster_care",
                "family_services",
                "child_welfare",
                "care_episodes",
                "family_relationships",
            ],
            entities=[
                "child",
                "mother",
                "father",
                "caregiver",
                "case_worker",
                "care_placement",
            ],
            join_keys=[
                "insight_id",
                "child_insight_id",
                "mother_insight_id",
                "father_insight_id",
                "ssn",
                "agency_id",
                "child_id",
            ],
            capabilities=[
                "lookup",
                "aggregate",
                "relationship",
                "family_tree",
                "care_history",
                "risk_assessment",
            ],
            security_level="confidential",
            last_updated=datetime.utcnow(),
        )

    @staticmethod
    def get_idjc_contract() -> AgencyCapability:
        """
        Get IDJC (Idaho Department of Juvenile Corrections) agent contract.

        IDJC manages juvenile correction records including:
        - Commitment details and release information
        - Offense information and charges
        - Custody/detention status
        - County information
        """
        return AgencyCapability(
            agent_id="agent-idjc-001",
            agency=AgencyName.IDJC,
            version="1.0.0",
            description="Idaho Department of Juvenile Corrections - Youth Justice",
            data_domain=[
                "juvenile_corrections",
                "youth_justice",
                "offenses",
                "commitments",
                "custody",
                "detention",
            ],
            entities=[
                "youth",
                "offense",
                "commitment",
                "detention",
                "county",
                "facility",
            ],
            join_keys=[
                "insight_id",
                "ssn",
                "first_name",
                "last_name",
                "dob",
                "ijos_id",
            ],
            capabilities=[
                "lookup",
                "aggregate",
                "offense_history",
                "commitment_status",
                "risk_assessment",
                "location_tracking",
            ],
            security_level="confidential",
            last_updated=datetime.utcnow(),
        )

    @staticmethod
    def get_idoc_contract() -> AgencyCapability:
        """
        Get IDOC (Idaho Department of Corrections) agent contract.

        IDOC manages adult incarceration records including:
        - Inmate demographics
        - Offense and sentencing information
        - Custody status and location
        - Interstate compact details
        """
        return AgencyCapability(
            agent_id="agent-idoc-001",
            agency=AgencyName.IDOC,
            version="1.0.0",
            description="Idaho Department of Corrections - Adult Incarceration",
            data_domain=[
                "corrections",
                "incarceration",
                "offenses",
                "sentencing",
                "custody",
                "interstate_compact",
            ],
            entities=[
                "inmate",
                "offense",
                "sentence",
                "custody_location",
                "facility",
                "county",
            ],
            join_keys=[
                "insight_id",
                "ssn_nbr",
                "ofndr_num",
                "fnam",
                "lnam",
                "dob_dtd",
            ],
            capabilities=[
                "lookup",
                "aggregate",
                "custody_status",
                "offense_history",
                "sentencing_details",
                "location_tracking",
                "interstate_compact_status",
            ],
            security_level="confidential",
            last_updated=datetime.utcnow(),
        )

    @classmethod
    def get_all_contracts(cls) -> dict[AgencyName, AgencyCapability]:
        """Get all agency contracts as a dictionary."""
        return {
            AgencyName.IDHW: cls.get_idhw_contract(),
            AgencyName.IDJC: cls.get_idjc_contract(),
            AgencyName.IDOC: cls.get_idoc_contract(),
        }

    @classmethod
    def get_contract(cls, agency: AgencyName) -> AgencyCapability:
        """
        Get contract for a specific agency.

        Args:
            agency: Agency name

        Returns:
            AgencyCapability for the specified agency

        Raises:
            ValueError: If agency is not recognized
        """
        contracts = cls.get_all_contracts()
        if agency not in contracts:
            raise ValueError(f"Unknown agency: {agency}")
        return contracts[agency]
