import requests
import time
import sys
import subprocess
import pg8000.native
import ssl
import hashlib
import re

def install_libraries_in_current_env(libraries):
    """
    Installs a list of specified Python libraries within the currently active Python environment using pip.

    Args:
    libraries (list of str): A list containing the names of the Python libraries to be installed.
        Each element in the list should be a string specifying the name of the library as you would
        pass it to pip (e.g., 'numpy', 'pandas==1.1.5', 'git+https://github.com/user/repo.git#egg=package').

    Description:
    The function iterates over each library provided in the 'libraries' list and attempts to install it
    using the pip package manager which is accessed directly through the Python executable of the current environment.
    This approach ensures that the libraries are installed in the environment from which the script is being run,
    rather than any globally active Python environment.

    Outputs:
    The function prints out a message for each library indicating whether the installation was successful or not.
    If the installation of a library fails, it additionally prints the error message received from pip.

    Example:
    To use this function, simply pass a list of library names:
        install_libraries_in_current_env(['numpy', 'pandas==1.1.5', 'scikit-learn'])
    This will attempt to install numpy, a specific version of pandas, and scikit-learn in the current Python environment.
    """
    python_executable = sys.executable  # Get the path to the current environment's Python executable

    for library in libraries:
        result = subprocess.run([python_executable, "-m", "pip", "install", library],
                                capture_output=True, text=True)

        if result.returncode == 0:
            print(f"Successfully installed {library}")
        else:
            print(f"Failed to install {library}. Error: {result.stderr}")


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
    all_data = []
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
            all_data.extend(data['docs'])
            # Increment the page counter if there are more pages to fetch
            if page >= data['pages']:
                break
            page += 1
        else:
            break  # Exit if the response is not successful or complete

    return all_data

def connect_to_database(credentials):
    ssl_context = ssl._create_unverified_context()

    # Assume credentials is a dictionary with the necessary key-value pairs
    db_credentials = {
        "host": credentials["host"],
        "database": credentials["dbname"],  # Ensure this key is correctly named
        "user": credentials["user"],
        "password": credentials["password"],
        "port": credentials["port"],  # Adjust the port if necessary
        "ssl_context": ssl_context,  # Add the SSL context to the connection parameters
    }

    # Establish a connection to the database
    conn = pg8000.connect(**db_credentials)
    return conn

def query_to_dataframe(conn, query):
    """
    Executes a SQL query and returns the result as a Pandas DataFrame.

    Parameters:
    - conn: A database connection object.
    - query: A string containing the SQL query to be executed.

    Returns:
    - A Pandas DataFrame containing the query results.
    """
    # Execute the query
    cursor = conn.cursor()
    try:
        #print(query)
        cursor.execute(query)
        # Fetch the results
        result = cursor.fetchall()
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        # Convert to DataFrame
        df = pd.DataFrame(result, columns=columns)
        print(f'The query returned {len(df)} records')
    finally:
        cursor.close()

    return df

def normalize_phone_number(phone, preserve_plus=True, format_number=False):
    """
    Normalize phone numbers by removing non-numeric characters and optionally formatting.

    Args:
        phone (str): The phone number to be normalized.
        preserve_plus (bool): If True, preserve leading '+' in international phone numbers.
        format_number (bool): If True, format the number to E.164 standard if possible.

    Returns:
        str or None: A normalized phone number or None if input is NaN or cannot be normalized.
    """
    if pd.isna(phone):
        return None
    else:
        # Convert to string to handle numeric inputs
        phone_str = str(phone)
        
        # Handle preserving '+' for international numbers
        if preserve_plus and phone_str.startswith('+'):
            phone_str = '+' + re.sub(r'\D', '', phone_str[1:])
        else:
            phone_str = re.sub(r'\D', '', phone_str)

        # Optionally format to E.164 if it starts with '+' and has sufficient digits
        if format_number and phone_str.startswith('+') and len(phone_str) >= 8:
            return phone_str
        else:
            return phone_str

def process_json_data(raw_data, join_data=None, column_mappings=None, concat_fields=None, limit_fields=None):
    """
    Processes JSON data into a pandas DataFrame with optional renaming, concatenating,
    and limiting of fields, as well as optional joining with another DataFrame.

    Args:
        raw_data (list of dict): The raw JSON data to be processed.
        join_data (pd.DataFrame, optional): A DataFrame to join to the processed data.
        column_mappings (dict, optional): A dictionary mapping original column names to new column names.
        concat_fields (list of tuples, optional): List of tuples where each tuple is (new_field_name, list_of_fields_to_concat).
        limit_fields (list, optional): List of strings indicating which columns to keep in the final DataFrame.
        
    Example data inputs:
        join_data = pd.DataFrame({
            'user_id': [1, 2],
            'community': ['Community A', 'Community B']
        })
        column_mappings = {'user_id': 'id', 'phone': 'contact_number'}
        concat_fields = [('full_name', ['first_name', 'last_name'])]
        limit_fields = ['id', 'full_name', 'contact_number', 'community']
        
    Returns:
        pd.DataFrame: A DataFrame containing the processed data.
    """

    # Convert JSON data to DataFrame
    df = pd.json_normalize(raw_data) 

    # Rename columns if mappings are provided
    if column_mappings:
        df.rename(columns=column_mappings, inplace=True)  # Example: {'id': 'user_id'}

    # Concatenate fields if specified
    if concat_fields:
        for new_field, fields in concat_fields:
            df[new_field] = df[fields].agg(' '.join, axis=1)  # Example: ('full_name', ['first_name', 'last_name'])

    # Join with another DataFrame if provided
    if join_data is not None:
        # Example join_data might have columns 'user_id' and 'user_type'
        df = df.merge(join_data, on='common_column', how='left')  # Example: on='user_id'

    # Limit fields to a subset if specified
    if limit_fields:
        df = df[limit_fields]  # Example: ['user_id', 'full_name', 'user_type']

    return df

def make_request_with_retry(url, headers, params, max_retries=10, backoff_factor=3):
    """
    Makes a GET request with retries and exponential backoff.

    Args:
        url (str): The URL for the API request.
        headers (dict): Headers to be sent with the request.
        params (dict): Parameters to be included in the request.
        max_retries (int): Maximum number of retries.
        backoff_factor (float): Factor by which to multiply the delay for each retry.

    Returns:
        requests.Response: The response object from the requests library.
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            return response
        except requests.RequestException as e:
            print(f"Request failed: {e}, attempt {attempt + 1} of {max_retries}")
            if attempt < max_retries - 1:
                sleep_time = backoff_factor * (2 ** attempt)
                print(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                raise
