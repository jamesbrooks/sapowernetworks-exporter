"""SAPN authentication and data download module.

This module handles:
- Authentication with SA Power Networks portal (Salesforce Visualforce)
- Session management and cookie handling
- Downloading NEM12 interval meter data files via Visualforce Remoting
"""

import logging
import re
import time
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Configure module logger
logger = logging.getLogger(__name__)


class SAPNError(Exception):
    """Base exception for SAPN scraper errors."""
    pass


class SAPNAuthError(SAPNError):
    """Exception raised when authentication fails."""
    pass


class SAPNDownloadError(SAPNError):
    """Exception raised when data download fails."""
    pass


class SAPNScraper:
    """Scraper for SA Power Networks meter data portal.

    Handles authentication with the Salesforce Visualforce portal and
    downloading NEM12 interval meter data via Visualforce Remoting API.

    The portal uses Salesforce Visualforce with JavaScript-based redirects
    after login and Apex Remote for data downloads.

    Attributes:
        username: Portal login username (email)
        password: Portal login password
        nmi: National Meter Identifier to download data for
    """

    BASE_URL = "https://customer.portal.sapowernetworks.com.au"
    LOGIN_URL = f"{BASE_URL}/meterdata/CADSiteLogin"
    DATA_URL = f"{BASE_URL}/meterdata/CADRequestMeterData"
    REMOTING_URL = f"{BASE_URL}/meterdata/apexremote"

    # Retry configuration
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # seconds

    # Download configuration - portal splits large date ranges into chunks
    MAX_DOWNLOAD_JOBS = 8  # Portal uses 8 parallel jobs for large requests

    def __init__(self, username: str, password: str, nmi: str):
        """Initialize the scraper with credentials.

        Args:
            username: Portal login username (email)
            password: Portal login password
            nmi: National Meter Identifier to download data for
        """
        self.username = username
        self.password = password
        self.nmi = nmi
        self.session = requests.Session()
        self._authenticated = False

        # Remoting context extracted from data page
        self._remoting_context: Optional[dict] = None

        # Set common headers
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-AU,en;q=0.9,en-US;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
        })

    def _extract_form_data(self, html: str) -> dict:
        """Extract all hidden form inputs from HTML.

        Args:
            html: HTML content containing the form

        Returns:
            Dictionary of form field names to values
        """
        soup = BeautifulSoup(html, "html.parser")
        form_data = {}

        for inp in soup.find_all("input", {"type": "hidden"}):
            name = inp.get("name")
            value = inp.get("value", "")
            if name:
                form_data[name] = value

        logger.debug(f"Extracted {len(form_data)} hidden form fields")
        return form_data

    def _follow_js_redirects(self, response: requests.Response) -> requests.Response:
        """Follow JavaScript-based redirects in Salesforce responses.

        Salesforce Visualforce uses JavaScript redirects after login instead
        of HTTP redirects. This method parses and follows those redirects.

        Args:
            response: Initial response that may contain JS redirects

        Returns:
            Final response after following all redirects
        """
        max_redirects = 5

        for _ in range(max_redirects):
            if "window.location" not in response.text:
                break

            # Try different redirect patterns used by Salesforce
            patterns = [
                r"window\.location\.replace\('([^']+)'\)",
                r"window\.location\.href\s*=\s*'([^']+)'",
                r"window\.location\s*=\s*'([^']+)'",
            ]

            redirect_url = None
            for pattern in patterns:
                match = re.search(pattern, response.text)
                if match:
                    redirect_url = match.group(1)
                    break

            if not redirect_url:
                break

            # Handle relative URLs
            if redirect_url.startswith("/"):
                redirect_url = urljoin(self.BASE_URL, redirect_url)

            logger.debug(f"Following JS redirect to: {redirect_url[:80]}...")
            response = self.session.get(redirect_url, allow_redirects=True)

        return response

    def _extract_remoting_context(self, html: str) -> dict:
        """Extract Visualforce Remoting context from page JavaScript.

        The Visualforce page includes JavaScript configuration for Apex Remote
        RPC calls, including VID, CSRF tokens, and authorization headers.

        Args:
            html: HTML content containing JavaScript

        Returns:
            Dictionary with remoting context for downloadNMIData method

        Raises:
            SAPNDownloadError: If remoting context cannot be extracted
        """
        # Extract VID
        vid_match = re.search(r'"vid"\s*:\s*"([^"]+)"', html)
        if not vid_match:
            raise SAPNDownloadError("Could not find VID in page")

        vid = vid_match.group(1)

        # Extract CSRF and authorization for downloadNMIData method
        download_method = re.search(r'\{"name":"downloadNMIData"[^}]+\}', html)
        if not download_method:
            raise SAPNDownloadError("Could not find downloadNMIData method config")

        method_json = download_method.group(0)
        csrf_match = re.search(r'"csrf"\s*:\s*"([^"]+)"', method_json)
        auth_match = re.search(r'"authorization"\s*:\s*"([^"]+)"', method_json)

        if not csrf_match or not auth_match:
            raise SAPNDownloadError("Could not extract CSRF/authorization tokens")

        context = {
            "vid": vid,
            "ns": "",
            "ver": 35,
            "csrf": csrf_match.group(1),
            "authorization": auth_match.group(1),
        }

        logger.debug(f"Extracted remoting context: vid={vid}")
        return context

    def _retry_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Execute request with retry logic.

        Args:
            method: HTTP method (GET, POST)
            url: Request URL
            **kwargs: Additional arguments passed to requests

        Returns:
            Response object

        Raises:
            SAPNError: After all retries exhausted
        """
        last_error = None

        for attempt in range(self.MAX_RETRIES):
            try:
                if attempt > 0:
                    delay = self.RETRY_DELAY * (2 ** (attempt - 1))  # Exponential backoff
                    logger.info(f"Retry {attempt}/{self.MAX_RETRIES} after {delay}s delay")
                    time.sleep(delay)

                response = self.session.request(method, url, **kwargs)
                response.raise_for_status()
                return response

            except requests.RequestException as e:
                last_error = e
                logger.warning(f"Request failed (attempt {attempt + 1}): {e}")

        raise SAPNError(f"Request failed after {self.MAX_RETRIES} retries: {last_error}")

    def login(self) -> bool:
        """Authenticate with the SAPN portal.

        Performs the login process:
        1. GET login page to extract form data (including ViewState tokens)
        2. POST credentials with form data
        3. Follow JavaScript-based redirects to complete authentication

        Returns:
            True if authentication succeeded

        Raises:
            SAPNAuthError: If authentication fails
        """
        logger.info(f"Authenticating as {self.username}")

        try:
            # Step 1: GET login page to extract form data
            logger.debug(f"Fetching login page: {self.LOGIN_URL}")
            response = self._retry_request("GET", self.LOGIN_URL)

            # Extract form action URL
            soup = BeautifulSoup(response.text, "html.parser")
            form = soup.find("form")
            if not form:
                raise SAPNAuthError("Could not find login form")

            form_action = form.get("action", self.LOGIN_URL)
            logger.debug(f"Form action: {form_action}")

            # Extract all hidden form fields (includes ViewState tokens)
            form_data = self._extract_form_data(response.text)

            # Add login credentials
            form_data["loginPage:SiteTemplate:siteLogin:loginComponent:loginForm:username"] = self.username
            form_data["loginPage:SiteTemplate:siteLogin:loginComponent:loginForm:password"] = self.password
            form_data["loginPage:SiteTemplate:siteLogin:loginComponent:loginForm:loginButton"] = "Login"

            # Step 2: POST login credentials
            logger.debug("Submitting login form")
            response = self._retry_request(
                "POST",
                form_action,
                data=form_data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": self.LOGIN_URL,
                    "Origin": self.BASE_URL,
                },
                allow_redirects=True,
            )

            # Step 3: Follow JavaScript redirects (Salesforce uses JS redirects)
            response = self._follow_js_redirects(response)

            # Check if login succeeded by verifying we can access the data page
            # or by checking for session cookie
            if not any("sid" in cookie.name.lower() for cookie in self.session.cookies):
                # Try to access data page to verify authentication
                test_response = self.session.get(
                    f"{self.DATA_URL}?selNMI={self.nmi}",
                    allow_redirects=True
                )
                if "CADSiteLogin" in test_response.url or "loginForm" in test_response.text:
                    raise SAPNAuthError("Login failed - redirected back to login page")

            self._authenticated = True
            logger.info("Authentication successful")
            return True

        except SAPNAuthError:
            raise
        except Exception as e:
            logger.error(f"Login failed: {e}")
            raise SAPNAuthError(f"Login failed: {e}")

    def download_nem12(self, from_date: Optional[datetime] = None, to_date: Optional[datetime] = None) -> str:
        """Download NEM12 interval data via Visualforce Remoting.

        Performs the data download flow:
        1. GET data request page to extract Remoting context
        2. Call downloadNMIData via Visualforce Remoting API
        3. Assemble CSV content from response

        Args:
            from_date: Start date for data (default: 30 days ago)
            to_date: End date for data (default: today)

        Returns:
            CSV content as string (NEM12 format starting with "200,")

        Raises:
            SAPNDownloadError: If download fails
        """
        if not self._authenticated:
            raise SAPNDownloadError("Not authenticated - call login() first")

        logger.info(f"Downloading NEM12 data for NMI {self.nmi}")

        try:
            # Calculate date range
            if to_date is None:
                to_date = datetime.now()
            if from_date is None:
                from_date = to_date - timedelta(days=30)

            # Format dates as JavaScript toUTCString() format for Apex Remote
            # Example: "Thu, 01 Jan 1970 00:00:00 GMT"
            from_date_str = from_date.strftime("%a, %d %b %Y 00:00:00 GMT")
            to_date_str = to_date.strftime("%a, %d %b %Y 00:00:00 GMT")

            logger.debug(f"Date range: {from_date_str} to {to_date_str}")

            # Step 1: GET data request page to extract Remoting context
            data_page_url = f"{self.DATA_URL}?selNMI={self.nmi}"
            logger.debug(f"Fetching data request page: {data_page_url}")
            response = self._retry_request("GET", data_page_url)

            # Extract remoting context (VID, CSRF, authorization)
            self._remoting_context = self._extract_remoting_context(response.text)

            # Step 2: Call downloadNMIData via Remoting API
            csv_content = self._call_download_remoting(from_date_str, to_date_str)

            if not csv_content:
                raise SAPNDownloadError("No data returned from download")

            # Verify we got valid NEM12 data
            if not csv_content.strip().startswith("200,"):
                logger.warning(f"Unexpected data format, first 100 chars: {csv_content[:100]}")

            logger.info(f"Downloaded {len(csv_content)} bytes of NEM12 data")
            return csv_content

        except SAPNDownloadError:
            raise
        except Exception as e:
            logger.error(f"Download failed: {e}")
            raise SAPNDownloadError(f"Download failed: {e}")

    def _call_download_remoting(self, from_date_str: str, to_date_str: str) -> str:
        """Call downloadNMIData via Visualforce Remoting API.

        Args:
            from_date_str: Start date in toUTCString format
            to_date_str: End date in toUTCString format

        Returns:
            CSV content as string

        Raises:
            SAPNDownloadError: If the remoting call fails
        """
        if not self._remoting_context:
            raise SAPNDownloadError("Remoting context not initialized")

        # Build remoting payload
        # downloadNMIData params: nmi, company, startDate, endDate, reportType, meterType, jobId
        payload = {
            "action": "CADRequestMeterDataController",
            "method": "downloadNMIData",
            "data": [
                self.nmi,
                "",  # company (empty string)
                from_date_str,
                to_date_str,
                "Customer Access NEM12",  # reportType
                "Interval",  # meterType/selection
                0,  # jobId
            ],
            "type": "rpc",
            "tid": 1,
            "ctx": self._remoting_context,
        }

        logger.debug(f"Calling downloadNMIData remoting endpoint")

        response = self.session.post(
            self.REMOTING_URL,
            json=payload,
            headers={
                "Content-Type": "application/json",
                "Referer": f"{self.DATA_URL}?selNMI={self.nmi}",
                "Origin": self.BASE_URL,
            },
        )

        if response.status_code != 200:
            raise SAPNDownloadError(f"Remoting call failed with status {response.status_code}")

        result = response.json()

        if not result or len(result) == 0:
            raise SAPNDownloadError("Empty response from remoting call")

        result_data = result[0]

        if result_data.get("statusCode") != 200:
            error_msg = result_data.get("message", "Unknown error")
            raise SAPNDownloadError(f"Remoting call error: {error_msg}")

        # Extract CSV data from result
        download_result = result_data.get("result", {})

        if isinstance(download_result, dict):
            csv_content = download_result.get("results", "")
            if csv_content:
                logger.debug(f"Received data with {download_result.get('numberStreams', 'unknown')} streams")
                return csv_content
            else:
                raise SAPNDownloadError("No results in download response")
        elif isinstance(download_result, str):
            return download_result
        else:
            raise SAPNDownloadError(f"Unexpected result type: {type(download_result)}")

    def scrape(self, from_date: Optional[datetime] = None, to_date: Optional[datetime] = None) -> str:
        """Complete scrape flow: authenticate and download data.

        This is the main entry point that combines login and download.

        Args:
            from_date: Start date for data (default: 30 days ago)
            to_date: End date for data (default: today)

        Returns:
            CSV content as string

        Raises:
            SAPNAuthError: If authentication fails
            SAPNDownloadError: If download fails
        """
        logger.info(f"Starting scrape for NMI {self.nmi}")

        self.login()
        csv_content = self.download_nem12(from_date=from_date, to_date=to_date)

        logger.info(f"Scrape complete, received {len(csv_content)} bytes")
        return csv_content


def main():
    """Test the scraper with provided credentials."""
    import os
    import sys

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Try to load environment variables if dotenv is available
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    # Get credentials from environment or use test values
    username = os.getenv("SAPN_USERNAME", "your_email@example.com")
    password = os.getenv("SAPN_PASSWORD", "YOUR_PASSWORD")
    nmi = os.getenv("SAPN_NMI", "YOUR_NMI")

    print(f"Testing SAPN scraper for NMI: {nmi}")
    print("=" * 60)

    scraper = SAPNScraper(username, password, nmi)

    try:
        # Test login
        print("\n1. Testing authentication...")
        scraper.login()
        print("   Authentication successful!")

        # Test download (last 30 days)
        print("\n2. Testing data download (last 30 days)...")
        csv_content = scraper.download_nem12()
        print(f"   Downloaded {len(csv_content)} bytes")

        # Verify data format
        lines = csv_content.strip().split("\n")
        print(f"   Number of lines: {len(lines)}")
        print(f"   First line: {lines[0][:100]}...")

        # Check that it starts with "200," (NEM12 data record)
        if csv_content.strip().startswith("200,"):
            print("   Data format: Valid NEM12 (starts with '200,')")
        else:
            print(f"   WARNING: Unexpected format, starts with: {csv_content[:20]}")

        print("\n" + "=" * 60)
        print("All tests passed!")

    except SAPNAuthError as e:
        print(f"\n   Authentication FAILED: {e}")
        return False
    except SAPNDownloadError as e:
        print(f"\n   Download FAILED: {e}")
        return False
    except Exception as e:
        print(f"\n   Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)
