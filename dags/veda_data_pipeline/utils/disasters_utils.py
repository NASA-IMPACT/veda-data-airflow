import re
import json
import boto3
from typing import Dict, List


def _load_from_s3(s3_key: str) -> dict:
    """Load JSON data from S3 using Airflow variables."""
    from airflow.models.variable import Variable
    bucket = Variable.get("aws_dags_variables", deserialize_json=True).get("EVENT_BUCKET")
    client = boto3.client("s3")
    result = client.get_object(Bucket=bucket, Key=s3_key)
    return json.loads(result["Body"].read().decode())


def _load_country_codes_mapping() -> Dict[str, List[str]]:
    """Load the monty country codes mapping from S3."""
    data = _load_from_s3("disasters-monty/monty_country_codes.json")
    return {entry["name"].lower(): entry["code"] for entry in data["country_codes"]}


def _load_hazard_codes_mapping() -> Dict[str, Dict[str, str]]:
    """Load the monty hazard codes mapping from S3."""
    data = _load_from_s3("disasters-monty/monty_hazard_codes.json")
    codes = data["classification_systems"]["undrr_isc_2025"]["codes"]
    return {
        entry["event_name"].lower(): {
            "glide_code": entry["glide_code"],
            "classification_code": entry["classification_code"]
        }
        for entry in codes
    }


# Lazy-load mappings on first access
_COUNTRY_CODES_MAPPING = None
_HAZARD_CODES_MAPPING = None


def _get_country_codes_mapping() -> Dict[str, List[str]]:
    """Get country codes mapping, loading from S3 on first access."""
    global _COUNTRY_CODES_MAPPING
    if _COUNTRY_CODES_MAPPING is None:
        _COUNTRY_CODES_MAPPING = _load_country_codes_mapping()
    return _COUNTRY_CODES_MAPPING


def _get_hazard_codes_mapping() -> Dict[str, Dict[str, str]]:
    """Get hazard codes mapping, loading from S3 on first access."""
    global _HAZARD_CODES_MAPPING
    if _HAZARD_CODES_MAPPING is None:
        _HAZARD_CODES_MAPPING = _load_hazard_codes_mapping()
    return _HAZARD_CODES_MAPPING


def _parse_filename(filename: str) -> dict:
    """Parse filename pattern YYYYMM_<hazard>_<location> and return components."""
    match = re.match(r"^(\d{6})_([^_]+)_([^_]+)", filename)
    if not match:
        return {}
    return {
        "year_month": match.group(1),
        "hazard_type": match.group(2).lower(),
        "location": match.group(3).lower(),
        "full_event_name": f"{match.group(1)}_{match.group(2)}_{match.group(3)}"
    }


def extract_event_name_from_filename(filename: str) -> dict:
    """Extract event name in format YYYYMM_<hazard>_<location>."""
    parsed = _parse_filename(filename)
    return {"event:name": parsed["full_event_name"]} if parsed else {}


def extract_country_codes_from_filename(filename: str) -> dict:
    """Extract ISO 3166-1 alpha-3 country codes from location."""
    parsed = _parse_filename(filename)
    if not parsed:
        return {}
    codes = _get_country_codes_mapping().get(parsed["location"])
    return {"monty:country_codes": codes} if codes else {}


def extract_hazard_codes_from_filename(filename: str) -> dict:
    """Extract GLIDE and UNDRR-ISC hazard classification codes."""
    parsed = _parse_filename(filename)
    if not parsed:
        return {}
    hazard = _get_hazard_codes_mapping().get(parsed["hazard_type"])
    return {"monty:hazard_codes": [hazard["glide_code"], hazard["classification_code"]]} if hazard else {}


def extract_datetime_from_filename(filename: str) -> str:
    """Extract datetime in formats: YYYY-MM-DD, YYYYMMDD, YYYY-MM-DDTHH:MM:SSZ, YYYYMMDDTHHMMSSZ."""
    patterns = [
        (r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)", lambda m: m.replace("-", "").replace(":", "")),
        (r"(\d{8}T\d{6}[Zz])", lambda m: m.upper()),
        (r"(\d{4}-\d{2}-\d{2})", lambda m: m.replace("-", "")),
        (r"(\d{8})", lambda m: m),
    ]
    for pattern, formatter in patterns:
        if match := re.search(pattern, filename):
            return formatter(match.group(1))
    return ""


def extract_corr_id_from_filename(filename: str) -> dict:
    """Extract correlation ID in format: {datetime}-{ISO3}-{GLIDE_CODE}-1-GCDB."""
    parsed = _parse_filename(filename)
    if not parsed:
        return {}
    datetime_str = extract_datetime_from_filename(filename)
    if not datetime_str:
        return {}
    location = parsed["location"]
    country_mapping = _get_country_codes_mapping()
    if location not in country_mapping:
        return {}
    hazard_type = parsed["hazard_type"]
    hazard_mapping = _get_hazard_codes_mapping()
    if hazard_type not in hazard_mapping:
        return {}
    country_code = country_mapping[location][0]
    glide_code = hazard_mapping[hazard_type]["glide_code"]
    return {"monty:corr_id": f"{datetime_str}-{country_code}-{glide_code}-1-GCDB"}


def extract_all_metadata_from_filename(filename: str) -> dict:
    """Extract all metadata fields: event:name, monty:country_codes, monty:hazard_codes, monty:corr_id."""
    parsed = _parse_filename(filename)
    if not parsed:
        return {}
    datetime_str = extract_datetime_from_filename(filename)
    if not datetime_str:
        return {}
    location = parsed["location"]
    country_mapping = _get_country_codes_mapping()
    if location not in country_mapping:
        return {}
    hazard_type = parsed["hazard_type"]
    hazard_mapping = _get_hazard_codes_mapping()
    if hazard_type not in hazard_mapping:
        return {}
    country_codes = country_mapping[location]
    hazard_info = hazard_mapping[hazard_type]
    hazard_codes = [hazard_info["glide_code"], hazard_info["classification_code"]]
    corr_id = f"{datetime_str}-{country_codes[0]}-{hazard_info['glide_code']}-1-GCDB"
    return {
        "event:name": parsed["full_event_name"],
        "monty:country_codes": country_codes,
        "monty:hazard_codes": hazard_codes,
        "monty:corr_id": corr_id
    }
