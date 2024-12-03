import requests
import time

def fetch_token(base_url, login_endpoint, credentials, headers=None):
    """
    Generic function to login to a REST API and retrieve an authentication token.

    Args:
    - base_url (str): Base URL of the API.
    - login_endpoint (str): Endpoint for logging in (appended to the base URL).
    - credentials (dict): Credentials required for login, typically including username and password.
    - headers (dict, optional): Additional headers to include in the request. Default is JSON content type.

    Returns:
    - str: Authentication token if login is successful, None otherwise.
    """
    if headers is None:
        headers = {
            "Content-Type": "application/json"
        }

    # Construct the full URL for the login request
    full_url = f"{base_url}/{login_endpoint}"

    # Send the POST request with the provided credentials and headers
    response = requests.post(full_url, json=credentials, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Attempt to extract the token from the response
        try:
            token = response.json().get('token')
            print("Authentication successful, token obtained.")
            return token
        except KeyError:
            print("Failed to extract token from response.")
            return None
    else:
        print("Failed to authenticate.")
        print("Status Code:", response.status_code)
        print("Response:", response.text)
        return None

def make_request_with_retries(url, method, headers, params=None, data=None, max_retries=5, initial_delay=1):
    """
    Makes an HTTP request to the given URL with exponential backoff retries and supports multiple HTTP methods.

    Args:
    - url (str): The URL to which the request is sent.
    - method (str): HTTP method to use (e.g., 'GET', 'POST', 'PATCH', 'DELETE').
    - headers (dict): Headers to include in the request.
    - params (dict, optional): Query parameters for the request.
    - data (dict or str, optional): Body of the request for methods like POST or PUT.
    - max_retries (int): Maximum number of retries on failures.
    - initial_delay (int): Initial delay between retries in seconds.

    Returns:
    - response (requests.Response): Response object from requests library.
    """
    retry_count = 0
    delay = initial_delay

    while retry_count <= max_retries:
        try:
            response = requests.request(method, url, headers=headers, params=params, json=data)
            if response.status_code == 200:
                return response
            elif response.status_code in [429, 500, 502, 503, 504]:
                # These are typical HTTP status codes that suggest a retry might be successful
                time.sleep(delay)  # Wait before retrying
                delay *= 2  # Exponential backoff
                retry_count += 1
            else:
                # For non-retriable HTTP status, raise an exception
                response.raise_for_status()
        except requests.exceptions.RequestException as e:
            if retry_count >= max_retries:
                raise  # If max retries are reached, raise the last exception
            time.sleep(delay)
            delay *= 2
            retry_count += 1

    raise Exception(f"All retries failed for {url}")

def simple_get_with_pagination(base_url, token, initial_params, endpoint):
    """
    Fetch data from a specific API endpoint using pagination and supports HTTP methods via retries.

    Args:
    - base_url (str): Base URL of the API.
    - token (str): Authentication token.
    - initial_params (dict): Initial query parameters for the request.
    - endpoint (str): Specific endpoint to append to the base URL.

    Returns:
    - list: Aggregated data collected from all pages of the API endpoint.
    """
    all_alerts = []
    page = initial_params.get("page", 1)
    headers = {
        "Authorization": f"Bearer {token}"
    }

    while True:
        url = f"{base_url}{endpoint}"
        initial_params['page'] = page
        
        # Use 'GET' method explicitly
        response = make_request_with_retries(url, 'GET', headers, params=initial_params)

        if response and response.status_code == 200:
            data = response.json()
            all_alerts.extend(data['docs'])
            # Increment the page counter if there are more pages to fetch
            if page >= data['pages']:
                break
            page += 1
        else:
            break  # Exit if the response is not successful or complete

    return all_alerts
