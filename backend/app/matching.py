"""Deterministic employment and qualitative classification.

Numeric eligibility stays in PostgreSQL. These rules are the previous n8n
classification, including PASS, FAIL, and UNKNOWN. Resolved pairs become
matches. Only criteria this stage cannot decide are returned for Gemini.
"""

import json
import math
import re
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

# JavaScript \b is an ASCII word boundary unless the unicode flag is set.
_WORD = re.IGNORECASE | re.ASCII
UNCHECKABLE = re.compile(
    r"\b(ssn|social security|bank|photo id|government-issued|government issued|"
    r"address|reside|resident|residency|nevada|west virginia|citizen|citizenship|"
    r"visa|payment history|credit usage)\b",
    _WORD,
)
EMPLOYMENT = re.compile(
    r"\b(employ(?:ed|ment|er)?|salaried|self-employed|self employed|student|"
    r"retired|unemployed|occupation)\b",
    _WORD,
)
EMAIL = re.compile(r"\bemail\b", _WORD)
EMAIL_EXTRA = re.compile(r"\b(online|device|computer|phone|finalize|finalise)\b", _WORD)

_EMPLOYMENT_MAP = {
    "salaried": "salaried",
    "employed": "salaried",
    "fulltime": "salaried",
    "selfemployed": "self_employed",
    "retired": "retired",
    "unemployed": "unemployed",
    "student": "student",
    "contract": "contract",
    "contractor": "contract",
}


@dataclass
class CheckCounter:
    value: int = 0

    def next_id(self, user_record_id: int, product_id: int) -> str:
        check_id = f"{user_record_id}:{product_id}:{self.value}"
        self.value += 1
        return check_id


@dataclass
class CandidateBatch:
    matches: list[dict[str, Any]] = field(default_factory=list)
    pending: list[dict[str, Any]] = field(default_factory=list)
    llm_checks: list[dict[str, Any]] = field(default_factory=list)
    deterministic_remaining: int = 0
    qualitative_pairs: int = 0


def as_array(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None or value == "":
        return []
    if isinstance(value, str):
        trimmed = value.strip()
        if not trimmed:
            return []
        try:
            parsed = json.loads(trimmed)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def normalize_employment(value: Any) -> str | None:
    compact = re.sub(r"[\s_-]+", "", str(value or "").strip().lower())
    return _EMPLOYMENT_MAP.get(compact)


def accepted_list(value: Any) -> list[str]:
    return [item for item in (normalize_employment(entry) for entry in as_array(value)) if item]


def employment_decision(status: Any, accepted: Any) -> dict[str, str]:
    statuses = accepted_list(accepted)
    if not statuses:
        return {"decision": "UNKNOWN", "reason": "No accepted employment list was stated."}
    normalized = normalize_employment(status)
    if not normalized:
        return {
            "decision": "UNKNOWN",
            "reason": "Employment status could not be mapped, so it was not treated as a pass or a fail.",
        }
    if normalized in statuses:
        return {
            "decision": "PASS",
            "reason": "Employment status " + normalized + " is in the accepted list.",
        }
    return {
        "decision": "FAIL",
        "reason": (
            "Employment status "
            + normalized
            + " is outside the accepted list ("
            + ", ".join(statuses)
            + ")."
        ),
    }


def number_or_null(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def js_to_fixed(value: float, digits: int = 2) -> str:
    """Format like JavaScript Number.toFixed for finite values."""
    if not math.isfinite(value):
        return "NaN"
    quant = Decimal(1).scaleb(-digits)
    rounded = Decimal.from_float(abs(float(value))).quantize(
        quant, rounding=ROUND_HALF_UP
    )
    text = f"{rounded:.{digits}f}"
    if value < 0 and rounded != 0:
        return "-" + text
    return text


def js_number(value: float) -> str:
    if math.isfinite(value) and value.is_integer():
        return str(int(value))
    return json.dumps(value)


def numeric_parts(candidate: dict[str, Any]) -> dict[str, list[str]]:
    income_min = number_or_null(candidate.get("minimum_monthly_income"))
    credit_min = number_or_null(candidate.get("credit_score_min"))
    credit_max = number_or_null(candidate.get("credit_score_max"))
    age_min = number_or_null(candidate.get("age_min"))
    age_max = number_or_null(candidate.get("age_max"))
    currency = str(candidate.get("income_currency") or "USD").strip()
    passed: list[str] = []
    unstated: list[str] = []
    if income_min is None:
        if currency == "USD":
            unstated.append("minimum income")
        else:
            unstated.append(
                "minimum income (" + currency + " was not compared to the CSV)"
            )
    else:
        income = js_to_fixed(float(candidate["monthly_income"]))
        passed.append(
            "monthly income "
            + income
            + " >= "
            + js_to_fixed(income_min)
            + " "
            + currency
        )
    if credit_min is None and credit_max is None:
        unstated.append("credit-score limits")
    else:
        if credit_min is not None:
            passed.append(
                "credit score "
                + str(candidate.get("credit_score"))
                + " >= "
                + js_number(credit_min)
            )
        if credit_max is not None:
            passed.append(
                "credit score "
                + str(candidate.get("credit_score"))
                + " <= "
                + js_number(credit_max)
            )
    if age_min is None and age_max is None:
        unstated.append("age limits")
    else:
        if age_min is not None:
            passed.append(
                "age " + str(candidate.get("age")) + " >= " + js_number(age_min)
            )
        if age_max is not None:
            passed.append(
                "age " + str(candidate.get("age")) + " <= " + js_number(age_max)
            )
    return {"passed": passed, "unstated": unstated}


def classify_criterion(criterion: Any) -> str:
    text = str(criterion or "").strip()
    if not text:
        return "unknown"
    uncheckable = UNCHECKABLE.search(text) is not None
    employment = EMPLOYMENT.search(text) is not None
    if employment:
        return "llm_limited" if uncheckable else "llm"
    if EMAIL.search(text) and EMAIL_EXTRA.search(text) is None and not uncheckable:
        return "email"
    return "unknown"


def criterion_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict) and isinstance(value.get("criterion"), str):
        return value["criterion"].strip()
    return ""


def render_reason(
    label: str,
    passed: list[str],
    unstated: list[str],
    employment_reason: str,
    qualitative_pass: list[str],
    qualitative_unknown: list[str],
) -> str:
    sentences = [
        label
        + ". Likely eligible based on the supplied CSV data. This is not a guaranteed lender approval.",
    ]
    if passed:
        sentences.append("Passed: " + "; ".join(passed) + ".")
    else:
        sentences.append("No numeric product limits were stated.")
    if unstated:
        sentences.append("Not stated by the product: " + ", ".join(unstated) + ".")
    sentences.append("Employment: " + employment_reason)
    if qualitative_pass:
        sentences.append(
            "Qualitative checks passed from supplied data: "
            + "; ".join(qualitative_pass)
            + "."
        )
    if qualitative_unknown:
        sentences.append(
            "Not verified because the CSV does not contain that information: "
            + "; ".join(qualitative_unknown)
            + "."
        )
    return " ".join(sentences)[:1800]


def _record_id(value: Any) -> int:
    return int(value)


def evaluate_candidates(
    candidates: list[dict[str, Any]],
    counter: CheckCounter | None = None,
) -> CandidateBatch:
    checks = counter or CheckCounter()
    result = CandidateBatch()
    for candidate in candidates:
        employment = employment_decision(
            candidate.get("employment_status"),
            candidate.get("accepted_employment_statuses"),
        )
        if employment["decision"] == "FAIL":
            continue
        result.deterministic_remaining += 1
        criteria = [
            text
            for text in (
                criterion_text(item)
                for item in as_array(candidate.get("qualitative_criteria"))
            )
            if text
        ]
        if criteria:
            result.qualitative_pairs += 1
        numeric = numeric_parts(candidate)
        qualitative_pass: list[str] = []
        qualitative_unknown: list[str] = []
        llm_for_pair: list[dict[str, Any]] = []
        for criterion in criteria:
            kind = classify_criterion(criterion)
            if kind == "email":
                email = str(candidate.get("email") or "")
                if "@" in email:
                    qualitative_pass.append(criterion + " (an email address is present)")
                else:
                    qualitative_unknown.append(criterion + " (the email value was not usable)")
                continue
            if kind == "unknown":
                qualitative_unknown.append(criterion)
                continue
            pass_allowed = kind == "llm"
            if employment["decision"] == "PASS" and pass_allowed:
                qualitative_pass.append(
                    criterion + " (covered by the structured employment list)"
                )
                continue
            if employment["decision"] == "PASS" and not pass_allowed:
                qualitative_unknown.append(criterion)
                continue
            llm_for_pair.append({"criterion": criterion, "pass_allowed": pass_allowed})

        user_record_id = _record_id(candidate["user_record_id"])
        product_id = _record_id(candidate["product_id"])
        label = str(
            candidate.get("product_name")
            or candidate.get("source_name")
            or "Loan product"
        )
        if not llm_for_pair:
            result.matches.append(
                {
                    "user_record_id": user_record_id,
                    "product_id": product_id,
                    "match_method": "deterministic",
                    "match_reason": render_reason(
                        label,
                        numeric["passed"],
                        numeric["unstated"],
                        employment["reason"],
                        qualitative_pass,
                        qualitative_unknown,
                    ),
                }
            )
            continue

        pair_checks = []
        employment_status = candidate.get("employment_status") or ""
        employment_status = "" if employment_status is None else str(employment_status)
        for check in llm_for_pair:
            check_id = checks.next_id(user_record_id, product_id)
            result.llm_checks.append(
                {
                    "check_id": check_id,
                    "user_record_id": user_record_id,
                    "product_id": product_id,
                    "employment_status": employment_status,
                    "criterion": check["criterion"],
                    "pass_allowed": check["pass_allowed"],
                }
            )
            pair_checks.append(
                {
                    "check_id": check_id,
                    "criterion": check["criterion"],
                    "pass_allowed": check["pass_allowed"],
                }
            )
        result.pending.append(
            {
                "label": label,
                "passed": numeric["passed"],
                "unstated": numeric["unstated"],
                "employment_reason": employment["reason"],
                "qualitative_pass": qualitative_pass,
                "qualitative_unknown": qualitative_unknown,
                "user_record_id": user_record_id,
                "product_id": product_id,
                "checks": pair_checks,
            }
        )
    return result


def llm_call_count(check_count: int) -> int:
    if check_count <= 0:
        return 0
    return (check_count + 24) // 25
