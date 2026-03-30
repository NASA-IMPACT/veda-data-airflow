import re
import json
import boto3
import rasterio
from functools import lru_cache
from typing import Dict, List, Optional, Any


def _load_from_s3(s3_key: str) -> dict:
    """Load JSON data from S3 using Airflow variables."""
    from airflow.models.variable import Variable
    bucket = Variable.get("aws_dags_variables", deserialize_json=True).get("EVENT_BUCKET")
    client = boto3.client("s3")
    result = client.get_object(Bucket=bucket, Key=s3_key)
    return json.loads(result["Body"].read().decode())


@lru_cache(maxsize=1)
def _get_country_codes_mapping() -> Dict[str, List[str]]:
    """Load and cache country codes mapping from S3."""
    data = _load_from_s3("monty/monty_country_codes.json")
    return {entry["name"].lower(): entry["code"] for entry in data["country_codes"]}


@lru_cache(maxsize=1)
def _get_hazard_codes_mapping() -> Dict[str, Dict[str, str]]:
    """Load and cache hazard codes mapping from S3."""
    data = _load_from_s3("monty/monty_hazard_codes.json")
    codes = data["classification_systems"]["undrr_isc_2025"]["codes"]
    return {
        entry["event_name"].lower(): {
            "glide_code": entry["glide_code"],
            "classification_code": entry["classification_code"]
        }
        for entry in codes
    }


@lru_cache(maxsize=1)
def _get_event_hazard_location_mapping() -> Dict[str, Dict[str, List[str]]]:
    """Load and cache event-hazard-location mapping from S3."""
    data = _load_from_s3("events/event-hazard-location.json")
    return data


def _read_geotiff_metadata(file_path: str) -> dict:
    """Read metadata from GeoTIFF file and convert all keys to lowercase for case-insensitive access.

    Args:
        file_path: Full path to the GeoTIFF file

    Returns:
        Dictionary with lowercase keys, or empty dict if error
    """
    try:
        with rasterio.open(file_path) as src:
            metadata = src.tags()
            # Convert all keys to lowercase for case-insensitive access
            return {k.lower(): v for k, v in metadata.items()}
    except Exception as e:
        print(f"Error reading GeoTIFF metadata from {file_path}: {e}")
        return {}


def _read_geotiff_event(file_path: str) -> Optional[str]:
    """Read EVENT metadata from GeoTIFF file attributes (case-insensitive)."""
    metadata = _read_geotiff_metadata(file_path)
    return metadata.get("event") if metadata else None


def _parse_event_string(event: str) -> dict:
    """Parse EVENT string in format YYYYMM_<hazard>_<location> and return components."""
    if not event:
        return {}
    match = re.match(r"^(\d{6})_([^_]+)_(.+)$", event)
    if not match:
        return {}
    return {
        "year_month": match.group(1),
        "hazard_type": match.group(2).lower(),
        "location": match.group(3).lower(),
        "full_event_name": event
    }


def extract_event_name_from_geotiff(file_path: str) -> dict:
    """Extract event name from GeoTIFF metadata."""
    event = _read_geotiff_event(file_path)
    if not event:
        return {"event:name": None}
    return {"event:name": event}


def extract_country_codes_from_geotiff(file_path: str) -> dict:
    """Extract ISO 3166-1 alpha-3 country codes from GeoTIFF EVENT metadata."""
    event = _read_geotiff_event(file_path)
    if not event:
        return {"monty:country_codes": None}
    parsed = _parse_event_string(event)
    if not parsed:
        return {"monty:country_codes": None}
    codes = _get_country_codes_mapping().get(parsed["location"])
    return {"monty:country_codes": codes if codes else None}


def extract_hazard_codes_from_geotiff(file_path: str) -> dict:
    """Extract GLIDE and UNDRR-ISC hazard classification codes from GeoTIFF EVENT metadata."""
    event = _read_geotiff_event(file_path)
    if not event:
        return {"monty:hazard_codes": None}
    parsed = _parse_event_string(event)
    if not parsed:
        return {"monty:hazard_codes": None}
    hazard = _get_hazard_codes_mapping().get(parsed["hazard_type"])
    return {"monty:hazard_codes": [hazard["glide_code"], hazard["classification_code"]] if hazard else None}


def extract_hazard_and_location_from_geotiff(file_path: str) -> dict:
    """Extract hazard and location arrays from GeoTIFF EVENT metadata using S3 mapping.

    Reads the EVENT field from GeoTIFF and looks it up in the event-hazard-location.json
    mapping to get the associated hazard types and locations.

    Args:
        file_path: Full path to the GeoTIFF file

    Returns:
        Dictionary with hazard and location keys, or None values if not found
    """
    event = _read_geotiff_event(file_path)
    if not event:
        return {"hazard": None, "location": None}

    event_mapping = _get_event_hazard_location_mapping()
    event_data = event_mapping.get(event)

    if not event_data:
        return {"hazard": None, "location": None}

    return {
        "hazard": event_data.get("hazard"),
        "location": event_data.get("location")
    }


def extract_providers_from_geotiff(file_path: str) -> dict:
    """Extract providers from GeoTIFF metadata (case-insensitive).

    Reads the PROVIDERS field from GeoTIFF metadata and converts the comma-separated
    string back into a list.

    Args:
        file_path: Full path to the GeoTIFF file

    Returns:
        Dictionary with providers key containing a list, or None if not found
    """
    metadata = _read_geotiff_metadata(file_path)
    providers_str = metadata.get("providers")
    if providers_str:
        # Split comma-separated providers back into a list
        return {"providers": [p.strip() for p in providers_str.split(",")]}
    return {"providers": None}


def extract_datetime_from_filename(filename: str) -> str:
    """Extract datetime from filename in formats: YYYY-MM-DD, YYYYMMDD, YYYY-MM-DDTHH:MM:SSZ, YYYYMMDDTHHMMSSZ."""
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


def extract_corr_id_from_geotiff(file_path: str) -> dict:
    """Extract correlation ID from GeoTIFF metadata in format: {datetime}-{ISO3}-{GLIDE_CODE}-1-GCDB.

    Note: Datetime is extracted from the filename, not from GeoTIFF metadata.
    """
    event = _read_geotiff_event(file_path)
    if not event:
        return {"monty:corr_id": None}
    parsed = _parse_event_string(event)
    if not parsed:
        return {"monty:corr_id": None}

    # Extract filename from path for datetime extraction
    filename = file_path.split("/")[-1]
    datetime_str = extract_datetime_from_filename(filename)
    if not datetime_str:
        return {"monty:corr_id": None}

    location = parsed["location"]
    country_mapping = _get_country_codes_mapping()
    if location not in country_mapping:
        return {"monty:corr_id": None}
    hazard_type = parsed["hazard_type"]
    hazard_mapping = _get_hazard_codes_mapping()
    if hazard_type not in hazard_mapping:
        return {"monty:corr_id": None}
    country_code = country_mapping[location][0]
    glide_code = hazard_mapping[hazard_type]["glide_code"]
    return {"monty:corr_id": f"{datetime_str}-{country_code}-{glide_code}-1-GCDB"}


def extract_all_metadata_from_geotiff(file_path: str) -> dict:
    """Extract all metadata fields from GeoTIFF: event:name, monty:country_codes, monty:hazard_codes, monty:corr_id, hazard, location, providers (case-insensitive)."""
    event = _read_geotiff_event(file_path)

    # Initialize result dict with None values
    result: Dict[str, Any] = {
        "event:name": None,
        "monty:country_codes": None,
        "monty:hazard_codes": None,
        "monty:corr_id": None,
        "hazard": None,
        "location": None,
        "providers": None
    }

    # Extract providers from GeoTIFF metadata (case-insensitive)
    metadata = _read_geotiff_metadata(file_path)
    providers_str = metadata.get("providers")
    if providers_str:
        # Split comma-separated providers back into a list
        result["providers"] = [p.strip() for p in providers_str.split(",")]

    if not event:
        return result

    # Set event name
    result["event:name"] = event

    # Extract hazard and location from event mapping
    event_mapping = _get_event_hazard_location_mapping()
    event_data = event_mapping.get(event)
    if event_data:
        result["hazard"] = event_data.get("hazard")
        result["location"] = event_data.get("location")

    parsed = _parse_event_string(event)
    if not parsed:
        return result

    # Extract filename from path for datetime extraction
    filename = file_path.split("/")[-1]
    datetime_str = extract_datetime_from_filename(filename)
    if not datetime_str:
        return result

    location = parsed["location"]
    country_mapping = _get_country_codes_mapping()
    if location not in country_mapping:
        return result

    hazard_type = parsed["hazard_type"]
    hazard_mapping = _get_hazard_codes_mapping()
    if hazard_type not in hazard_mapping:
        return result

    country_codes = country_mapping[location]
    hazard_info = hazard_mapping[hazard_type]
    hazard_codes = [hazard_info["glide_code"], hazard_info["classification_code"]]
    corr_id = f"{datetime_str}-{country_codes[0]}-{hazard_info['glide_code']}-1-GCDB"

    result["monty:country_codes"] = country_codes
    result["monty:hazard_codes"] = hazard_codes
    result["monty:corr_id"] = corr_id

    return result


def extract_sensor_and_product_from_path(file_path: str) -> dict:
    """Extract sensor and product from S3 path.

    Extracts the last 2 directory names before the filename from an S3 path.
    Example: s3://bucket/ProgramData/Sentinel-2/TrueColor/file.tif
    Returns: {"sensor": "Sentinel-2", "product": "TrueColor"}

    Args:
        file_path: Full S3 path to the file

    Returns:
        Dictionary with sensor and product keys, or None values if extraction fails
    """
    try:
        # Remove s3:// prefix if present and split path
        path = file_path.replace("s3://", "")
        parts = path.split("/")

        # Need at least 3 parts: bucket, sensor, product, filename
        if len(parts) < 3:
            return {"sensor": None, "product": None}

        # Get the last 2 directories before the filename
        # parts[-1] is filename, parts[-2] is product, parts[-3] is sensor
        sensor = parts[-3] if len(parts) >= 3 else None
        product = parts[-2] if len(parts) >= 2 else None

        return {
            "sensor": sensor,
            "product": product
        }
    except Exception as e:
        print(f"Error extracting sensor/product from path {file_path}: {e}")
        return {"sensor": None, "product": None}
