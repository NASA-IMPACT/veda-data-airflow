"""
STAC Collection Validation Utilities

This module provides validation functions for STAC collections using PySTAC's
native validation capabilities, which automatically validates against all declared
STAC extensions.
"""

import logging
from typing import Union

import pystac


def validate_collection(collection: Union[dict, pystac.Collection]) -> dict:
    """
    Validates a STAC collection using PySTAC's native validation.

    This function accepts either a dictionary representation of a collection or
    a pystac.Collection object, converts it to a pystac.Collection if needed,
    and validates it against the STAC specification and all declared extensions
    in the collection's `stac_extensions` field.

    Args:
        collection: Collection config as a dictionary or pystac.Collection object.
                   If a dict, it must be a valid STAC Collection JSON structure.

    Returns:
        dict: The validated collection as a dictionary. This is the same structure
              as the input (if dict) or the dict representation of the input
              (if pystac.Collection).

    Raises:
        ValueError: If the collection is invalid according to STAC specification
                   or any of its declared extensions. The error message will contain
                   details about the validation failure.
        TypeError: If the input is neither a dict nor a pystac.Collection.

    Example:
        >>> collection_dict = {
        ...     "type": "Collection",
        ...     "id": "my-collection",
        ...     "stac_version": "1.0.0",
        ...     "description": "Example collection",
        ...     "license": "proprietary",
        ...     "extent": {...},
        ...     "stac_extensions": [
        ...         "https://stac-extensions.github.io/web-map-links/v1.2.0/schema.json"
        ...     ],
        ...     "links": [...]
        ... }
        >>> validated = validate_collection(collection_dict)
        >>> print("Collection is valid!")
    """
    logger = logging.getLogger(__name__)

    # Determine if we need to convert from dict to pystac.Collection
    collection_obj = None
    is_dict_input = False

    if isinstance(collection, dict):
        is_dict_input = True
        collection_id = collection.get('id', 'unknown')
        logger.info(f"Validating collection '{collection_id}' from dictionary")

        try:
            collection_obj = pystac.Collection.from_dict(collection)
        except Exception as e:
            error_msg = f"Failed to convert collection dictionary to pystac.Collection: {str(e)}"
            raise ValueError(error_msg) from e

    elif isinstance(collection, pystac.Collection):
        collection_obj = collection
        collection_id = collection.id

    else:
        error_msg = f"Invalid input type: expected dict or pystac.Collection, got {type(collection).__name__}"
        raise TypeError(error_msg)

    # Validate the collection using pystac's native validation
    # This automatically validates against all stac_extensions declared in the collection
    try:
        num_extensions = len(collection_obj.stac_extensions) if collection_obj.stac_extensions else 0

        # Get STAC version from the collection dict representation
        stac_version = collection_obj.to_dict().get('stac_version', 'unknown')

        logger.debug(
            f"Validating collection '{collection_id}' against STAC {stac_version} "
            f"with {num_extensions} extension(s)"
        )

        # Perform validation
        collection_obj.validate()

        logger.info(
            f"Collection '{collection_id}' is valid according to STAC {stac_version} "
            f"specification" + (f" and {num_extensions} extension(s)" if num_extensions > 0 else "")
        )

    except pystac.STACValidationError as e:
        error_msg = f"Collection '{collection_id}' validation failed: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Validation error details: {e}")
        raise ValueError(error_msg) from e

    except Exception as e:
        error_msg = f"Unexpected error during validation of collection '{collection_id}': {str(e)}"
        logger.error(error_msg)
        raise ValueError(error_msg) from e

    # Return as dictionary for consistency with existing codebase patterns
    validated_dict = collection_obj.to_dict()

    return validated_dict
