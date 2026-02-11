"""
Tools module for the Companies House MCP server.

This module defines the MCP tools for accessing UK company registration data
via the Companies House API.
"""

import base64
import os

import requests

TIMEOUT = 30
BASE_URL = "https://api.company-information.service.gov.uk"


def _get_auth_header() -> str | None:
    """Get the Basic auth header for Companies House API."""
    api_key = os.environ.get("COMPANIES_HOUSE_API_KEY")
    if not api_key:
        return None
    return "Basic " + base64.b64encode((api_key + ":").encode("utf-8")).decode("utf-8")


def load_tools(mcp_server):
    """
    Register all Companies House MCP tools with the server.

    Args:
        mcp_server: The FastMCP server instance to register tools with.
    """

    @mcp_server.tool
    def health() -> dict:
        """
        Check the health of the Companies House MCP server.

        Returns:
            dict: Health status information.
        """
        api_key = os.environ.get("COMPANIES_HOUSE_API_KEY")
        return {
            "status": "healthy",
            "message": "Companies House MCP Server is running.",
            "api_key_configured": bool(api_key),
        }

    @mcp_server.tool
    def search_companies(
        query: str,
        items_per_page: int = 10,
        start_index: int = 0,
    ) -> dict:
        """
        Search for UK companies by name or keyword.

        Args:
            query: Search query (company name or keyword).
            items_per_page: Number of results per page (default: 10, max: 100).
            start_index: Starting index for pagination (default: 0).

        Returns:
            dict: Search results containing company names, numbers, and basic info.

        Example:
            search_companies("Databricks", items_per_page=5)
        """
        auth_header = _get_auth_header()
        if not auth_header:
            return {"error": "COMPANIES_HOUSE_API_KEY environment variable not set"}

        url = f"{BASE_URL}/search/companies"
        params = {
            "q": query,
            "items_per_page": str(items_per_page),
            "start_index": str(start_index),
        }
        headers = {"Authorization": auth_header}

        try:
            response = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            return {"error": f"Request timed out after {TIMEOUT} seconds"}
        except requests.exceptions.HTTPError:
            return {"error": f"HTTP {response.status_code}", "message": response.text}
        except Exception as e:
            return {"error": "Request failed", "message": str(e)}

    @mcp_server.tool
    def get_company_profile(company_number: str) -> dict:
        """
        Get detailed profile information for a specific company.

        Args:
            company_number: The UK company registration number (e.g., "14307029").

        Returns:
            dict: Detailed company information including registered address,
                  officers, filing history, and company status.

        Example:
            get_company_profile("14307029")
        """
        auth_header = _get_auth_header()
        if not auth_header:
            return {"error": "COMPANIES_HOUSE_API_KEY environment variable not set"}

        url = f"{BASE_URL}/company/{company_number}"
        headers = {"Authorization": auth_header}

        try:
            response = requests.get(url, headers=headers, timeout=TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            return {"error": f"Request timed out after {TIMEOUT} seconds"}
        except requests.exceptions.HTTPError:
            return {"error": f"HTTP {response.status_code}", "message": response.text}
        except Exception as e:
            return {"error": "Request failed", "message": str(e)}

    @mcp_server.tool
    def get_company_officers(
        company_number: str,
        items_per_page: int = 35,
        start_index: int = 0,
    ) -> dict:
        """
        Get the list of officers (directors, secretaries) for a company.

        Args:
            company_number: The UK company registration number.
            items_per_page: Number of results per page (default: 35).
            start_index: Starting index for pagination (default: 0).

        Returns:
            dict: List of company officers with their roles and appointment dates.

        Example:
            get_company_officers("14307029")
        """
        auth_header = _get_auth_header()
        if not auth_header:
            return {"error": "COMPANIES_HOUSE_API_KEY environment variable not set"}

        url = f"{BASE_URL}/company/{company_number}/officers"
        params = {
            "items_per_page": str(items_per_page),
            "start_index": str(start_index),
        }
        headers = {"Authorization": auth_header}

        try:
            response = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            return {"error": f"Request timed out after {TIMEOUT} seconds"}
        except requests.exceptions.HTTPError:
            return {"error": f"HTTP {response.status_code}", "message": response.text}
        except Exception as e:
            return {"error": "Request failed", "message": str(e)}

    @mcp_server.tool
    def get_filing_history(
        company_number: str,
        items_per_page: int = 25,
        start_index: int = 0,
    ) -> dict:
        """
        Get the filing history for a company.

        Args:
            company_number: The UK company registration number.
            items_per_page: Number of results per page (default: 25).
            start_index: Starting index for pagination (default: 0).

        Returns:
            dict: List of company filings with dates and document types.

        Example:
            get_filing_history("14307029", items_per_page=10)
        """
        auth_header = _get_auth_header()
        if not auth_header:
            return {"error": "COMPANIES_HOUSE_API_KEY environment variable not set"}

        url = f"{BASE_URL}/company/{company_number}/filing-history"
        params = {
            "items_per_page": str(items_per_page),
            "start_index": str(start_index),
        }
        headers = {"Authorization": auth_header}

        try:
            response = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            return {"error": f"Request timed out after {TIMEOUT} seconds"}
        except requests.exceptions.HTTPError:
            return {"error": f"HTTP {response.status_code}", "message": response.text}
        except Exception as e:
            return {"error": "Request failed", "message": str(e)}
