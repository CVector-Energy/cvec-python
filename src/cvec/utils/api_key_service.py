"""
API Key Service

Loads host-to-API-key mappings from AWS Secrets Manager or local environment variables.
Supports both AWS Secrets Manager (for production) and local JSON environment variables (for development).
"""

import json
import logging
import os
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Global cache for API keys (loaded once per process/cold start)
_API_KEYS_CACHE: Optional[Dict[str, str]] = None


def load_api_keys() -> Optional[Dict[str, str]]:
    """
    Load host-to-API-key mapping from AWS Secrets Manager or local environment variables.
    Caches the mapping in a global variable to avoid repeated API calls.

    The loading priority is:
    1. AWS Secrets Manager (if AWS_API_KEYS_SECRET environment variable is set)
    2. Local JSON environment variable API_KEYS_MAPPING (for development)

    Returns:
        dict: Mapping of host URLs to API keys, or None if no source is available
    """
    global _API_KEYS_CACHE

    if _API_KEYS_CACHE is not None:
        logger.debug("Using cached API keys")
        return _API_KEYS_CACHE

    # Try AWS Secrets Manager first
    aws_secret_name = os.environ.get("AWS_API_KEYS_SECRET")
    if aws_secret_name:
        logger.info(
            f"AWS_API_KEYS_SECRET is set, loading from Secrets Manager: {aws_secret_name}"
        )
        api_keys = _load_from_aws_secrets_manager(aws_secret_name)
        if api_keys is not None:
            _API_KEYS_CACHE = api_keys
            return _API_KEYS_CACHE

    # Fall back to local environment variable
    local_mapping = os.environ.get("API_KEYS_MAPPING")
    if local_mapping:
        logger.info("Loading API keys from local environment variable API_KEYS_MAPPING")
        api_keys = _load_from_local_env(local_mapping)
        if api_keys is not None:
            _API_KEYS_CACHE = api_keys
            return _API_KEYS_CACHE

    logger.warning(
        "No API key mapping found. Set either AWS_API_KEYS_SECRET or API_KEYS_MAPPING environment variable."
    )
    return None


def _load_from_aws_secrets_manager(secret_name: str) -> Optional[Dict[str, str]]:
    """
    Load API keys from AWS Secrets Manager.

    Args:
        secret_name: The name/ARN of the secret in AWS Secrets Manager

    Returns:
        dict: Mapping of host URLs to API keys, or None if loading fails
    """
    try:
        import boto3  # type: ignore[import-untyped]

        secretsmanager = boto3.client("secretsmanager")

        response = secretsmanager.get_secret_value(SecretId=secret_name)

        # Check if SecretString exists and is not empty
        if "SecretString" not in response:
            logger.warning(
                f"Secret '{secret_name}' exists but has no value. Please populate it."
            )
            return None

        secret_string = response["SecretString"]

        # Check if the secret string is empty or whitespace
        if not secret_string or not secret_string.strip():
            logger.warning(f"Secret '{secret_name}' is empty. Please populate it.")
            return None

        api_keys: Dict[str, str] = json.loads(secret_string)
        logger.info(
            f"Loaded API keys for {len(api_keys)} hosts from AWS Secrets Manager"
        )
        return api_keys

    except json.JSONDecodeError as e:
        logger.error(f"Secret '{secret_name}' contains invalid JSON: {str(e)}")
        return None
    except Exception as e:
        # Generic catch-all for any other errors (e.g., ResourceNotFoundException, network errors, etc.)
        logger.error(
            f"Error loading API keys from AWS Secrets Manager: {str(e)}",
            exc_info=True,
        )
        return None


def _load_from_local_env(json_string: str) -> Optional[Dict[str, str]]:
    """
    Load API keys from local environment variable containing JSON.

    Args:
        json_string: JSON string containing host-to-API-key mapping

    Returns:
        dict: Mapping of host URLs to API keys, or None if parsing fails
    """
    try:
        api_keys = json.loads(json_string)
        if not isinstance(api_keys, dict):
            logger.error(
                "API_KEYS_MAPPING must be a JSON object/dict mapping hosts to API keys"
            )
            return None

        logger.info(f"Loaded API keys for {len(api_keys)} hosts from local environment")
        return api_keys

    except json.JSONDecodeError as e:
        logger.error(f"API_KEYS_MAPPING contains invalid JSON: {str(e)}")
        return None


def get_api_key_for_host(host: str) -> Optional[str]:
    """
    Get the API key for a specific host.

    Args:
        host: The host URL

    Returns:
        str: The API key for the host, or None if not found

    Raises:
        ValueError: If no API keys mapping is available
    """
    api_keys = load_api_keys()

    if api_keys is None:
        raise ValueError(
            "No API keys mapping available. Set either AWS_API_KEYS_SECRET or "
            "API_KEYS_MAPPING environment variable."
        )

    api_key = api_keys.get(host)
    if not api_key:
        logger.warning(f"No API key found for host: {host}")
        return None

    return api_key
