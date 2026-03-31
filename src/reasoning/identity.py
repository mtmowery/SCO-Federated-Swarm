"""
Identity Resolution Agent (Phase 4)

Provides hash-based and fuzzy matching for cross-agency entity resolution
when insight_id is not available. For the MVP, insight_id provides
deterministic joins, but this module prepares for future fuzzy matching.

Techniques:
- Exact hash join (SHA256 of SSN + DOB)
- Name similarity (Jaro-Winkler)
- DOB matching
- Composite scoring with confidence threshold
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_CONFIDENCE_THRESHOLD = 0.90


@dataclass
class IdentityMatch:
    """A matched identity across agencies."""
    source_agency: str
    source_id: str
    target_agency: str
    target_id: str
    confidence: float
    match_method: str  # hash_exact, fuzzy_composite
    match_details: dict


class IdentityResolver:
    """
    Resolves entity matches across agencies.

    In the MVP with insight_id, this acts as a pass-through.
    In production (Phase 4+), it uses hash joins and fuzzy matching.
    """

    def __init__(self, confidence_threshold: float = DEFAULT_CONFIDENCE_THRESHOLD) -> None:
        self.confidence_threshold = confidence_threshold
        self.match_cache: dict[str, IdentityMatch] = {}

    # ── Deterministic Matching (MVP) ────────────────────────────

    def match_by_insight_id(
        self,
        source_records: list[dict],
        target_records: list[dict],
        source_agency: str,
        target_agency: str,
    ) -> list[IdentityMatch]:
        """
        Deterministic match using shared insight_id.
        This is the MVP path — 100% confidence, zero ambiguity.
        """
        target_index = {
            r.get("insight_id"): r
            for r in target_records
            if r.get("insight_id")
        }

        matches = []
        for source in source_records:
            src_id = source.get("insight_id")
            if src_id and src_id in target_index:
                match = IdentityMatch(
                    source_agency=source_agency,
                    source_id=src_id,
                    target_agency=target_agency,
                    target_id=src_id,
                    confidence=1.0,
                    match_method="insight_id_exact",
                    match_details={"shared_key": src_id},
                )
                matches.append(match)
                self.match_cache[src_id] = match

        logger.info(
            f"Identity resolution: {len(matches)} exact matches "
            f"between {source_agency} and {target_agency}"
        )
        return matches

    # ── Hash-Based Matching (Phase 4) ───────────────────────────

    @staticmethod
    def compute_join_token(ssn: str, dob: str, salt: str = "") -> str:
        """
        Privacy-preserving join token.
        Controller never sees raw PII — only hashed tokens.
        """
        raw = f"{ssn.strip()}|{dob.strip()}|{salt}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def match_by_hash(
        self,
        source_records: list[dict],
        target_records: list[dict],
        source_agency: str,
        target_agency: str,
        salt: str = "",
    ) -> list[IdentityMatch]:
        """
        Hash-based join using SHA256(SSN + DOB).
        This is the PPRL (Privacy Preserving Record Linkage) pattern.
        """
        # Build target hash index
        target_hashes: dict[str, dict] = {}
        for rec in target_records:
            ssn = str(rec.get("ssn", rec.get("ssn_nbr", rec.get("SSN", ""))))
            dob = str(rec.get("dob", rec.get("dob_dtd", rec.get("DOB", ""))))
            if ssn and dob:
                token = self.compute_join_token(ssn, dob, salt)
                target_hashes[token] = rec

        matches = []
        for rec in source_records:
            ssn = str(rec.get("ssn", rec.get("ssn_nbr", rec.get("SSN", ""))))
            dob = str(rec.get("dob", rec.get("dob_dtd", rec.get("DOB", ""))))
            if ssn and dob:
                token = self.compute_join_token(ssn, dob, salt)
                if token in target_hashes:
                    target = target_hashes[token]
                    match = IdentityMatch(
                        source_agency=source_agency,
                        source_id=rec.get("insight_id", ""),
                        target_agency=target_agency,
                        target_id=target.get("insight_id", ""),
                        confidence=0.98,  # Hash match is very high confidence
                        match_method="hash_exact",
                        match_details={"hash_prefix": token[:8] + "..."},
                    )
                    matches.append(match)

        logger.info(
            f"Hash-based resolution: {len(matches)} matches "
            f"between {source_agency} and {target_agency}"
        )
        return matches

    # ── Fuzzy Matching (Phase 4+) ───────────────────────────────

    @staticmethod
    def name_similarity(name_a: str, name_b: str) -> float:
        """Compute name similarity using SequenceMatcher."""
        if not name_a or not name_b:
            return 0.0
        return SequenceMatcher(
            None,
            name_a.upper().strip(),
            name_b.upper().strip(),
        ).ratio()

    def fuzzy_score(self, record_a: dict, record_b: dict) -> float:
        """
        Composite fuzzy match score across multiple attributes.

        Weights:
        - DOB exact match: 0.40
        - Name similarity: 0.35
        - Gender match: 0.10
        - SSN partial: 0.15
        """
        score = 0.0

        # DOB
        dob_a = str(record_a.get("dob", "")).strip()
        dob_b = str(record_b.get("dob", record_b.get("dob_dtd", ""))).strip()
        if dob_a and dob_b and dob_a == dob_b:
            score += 0.40

        # Name
        name_a = f"{record_a.get('first_name', '')} {record_a.get('last_name', '')}"
        name_b_first = record_b.get("first_name", record_b.get("fnam", record_b.get("FIRST_NAME", "")))
        name_b_last = record_b.get("last_name", record_b.get("lnam", record_b.get("LAST_NAME", "")))
        name_b = f"{name_b_first} {name_b_last}"
        score += 0.35 * self.name_similarity(name_a, name_b)

        # Gender
        gender_a = str(record_a.get("gender", "")).upper()[:1]
        gender_b = str(record_b.get("gender", record_b.get("sex_cd", record_b.get("GENDER", "")))).upper()[:1]
        if gender_a and gender_b and gender_a == gender_b:
            score += 0.10

        # SSN last 4
        ssn_a = str(record_a.get("ssn", ""))[-4:]
        ssn_b = str(record_b.get("ssn", record_b.get("ssn_nbr", record_b.get("SSN", ""))))[-4:]
        if len(ssn_a) == 4 and ssn_a == ssn_b:
            score += 0.15

        return round(score, 3)

    def fuzzy_match(
        self,
        source_records: list[dict],
        target_records: list[dict],
        source_agency: str,
        target_agency: str,
    ) -> list[IdentityMatch]:
        """
        Fuzzy match records across agencies using composite scoring.
        Only returns matches above the confidence threshold.
        """
        matches = []
        for src in source_records:
            best_score = 0.0
            best_target: dict = {}

            for tgt in target_records:
                score = self.fuzzy_score(src, tgt)
                if score > best_score:
                    best_score = score
                    best_target = tgt

            if best_score >= self.confidence_threshold:
                match = IdentityMatch(
                    source_agency=source_agency,
                    source_id=src.get("insight_id", ""),
                    target_agency=target_agency,
                    target_id=best_target.get("insight_id", ""),
                    confidence=best_score,
                    match_method="fuzzy_composite",
                    match_details={
                        "score": best_score,
                        "threshold": self.confidence_threshold,
                    },
                )
                matches.append(match)

        logger.info(
            f"Fuzzy resolution: {len(matches)} matches (threshold={self.confidence_threshold}) "
            f"between {source_agency} and {target_agency}"
        )
        return matches


# ── LangGraph Node Function ────────────────────────────────────

def identity_node(state: dict) -> dict:
    """
    LangGraph node: Identity Resolution.

    For MVP, this is a pass-through since insight_id provides
    deterministic joins. The node simply confirms match confidence.
    """
    trace = state.get("execution_trace", [])

    # In MVP mode, insight_id provides 100% match confidence
    state["identity_matches"] = {
        "method": "insight_id_exact",
        "confidence": 1.0,
        "note": "Pre-resolved insight_id used for all cross-agency joins.",
    }

    trace.append("[Identity] Using deterministic insight_id matching (confidence=1.0)")
    state["execution_trace"] = trace

    return state
