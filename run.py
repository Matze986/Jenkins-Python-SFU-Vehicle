import json
import sys
import subprocess
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# MinIO Configuration
MINIO_ENDPOINT = "http://host.docker.internal:9090/"
ACCESS_KEY = "IAB1NY18WjiqGmAMvhj8"
SECRET_KEY = "g7T5SB0INNVi5XqM5bpVrRDiWGQHgyX3Q1niH9Yj"
BUCKET_NAME = "test-bucket"

# Build form data object
def flatten_json(obj, prefix=''):
    """
    Recursively flattens a nested JSON-like structure (dicts and lists) 
    into key-value pairs using dot notation for dictionary keys and 
    bracket notation for list indices.

    Args:
        obj (dict, list, or other): The JSON-like object to be flattened.
        prefix (str): The accumulated key path, used for recursion.

    Returns:
        dict: A flattened dictionary with keys representing the nested structure.
    """
    flattened = {}

    match obj:
        case dict():
            for key, value in obj.items():
                new_prefix = f"{prefix}.{key}" if prefix else key
                flattened.update(flatten_json(value, new_prefix))
        case list():
            for index, value in enumerate(obj):
                new_prefix = f"{prefix}[{index}]"  # Use bracket notation for indices
                flattened.update(flatten_json(value, new_prefix))
        case _:  
            flattened[prefix] = obj

    return flattened

# Check BaseUrl
def get_base_urls(base_url):
    """
    Determines the appropriate base URLs based on the input URL.

    Args:
        base_url (str): The base URL to check.

    Returns:
        list: A list of base URLs. If 'localhost' is in base_url, returns 
              predefined Docker internal addresses; otherwise, returns 
              the original base_url as a single-item list.
    """
    if "localhost" in base_url:
        return ["http://host.docker.internal:5006", "http://host.docker.internal:9000"]
    return [base_url]

def build_form_data(parsed_data, url, http_method=None, file=None):
    """
    Constructs a cURL command using form data based on the given input.

    Args:
        parsed_data (dict): The JSON-like data to be flattened and included in the form.
        url (str): The endpoint URL to which the request will be sent.
        http_method (str, optional): The HTTP method to be used (e.g., POST, PUT).
        file_path (str, optional): The path to a file that needs to be uploaded.

    Returns:
        str: A string representing the cURL command with the form data.
    """
    print("Building form data object ...")
    
    # Flatten JSON
    flattened_data = flatten_json(parsed_data)
    try:
        # Generate the curl command using --form
        curl_command = f'curl -X "{http_method}" "{url}" \\\n'
        
        # Add custom headers
        curl_command += '  -H "x-forwarded-client-cert: Subject=\\"UID=DRMADMIN\\"" \\\n'

        for key, value in flattened_data.items():
            curl_command += f'  --form "{key}={value}" \\\n'

        # Added filename path if provided
        if file:
            curl_command += f'  --form "file=@{file}" \\\n'

        # Remove trailing backslash and newline
        curl_command = curl_command.rstrip(" \\\n")
        print("Building curl command completed.\n")
        
    except Exception:
        print(f"Something went wrong on building curl command: {Exception}")
        sys.exit(1)

    return curl_command

def sending_curl_command(curl_command):
    try:
        # Execute the curl command
        result = subprocess.run(
            ["curl", "-X", "GET", "https://api64.ipify.org?format=json"],
            capture_output=True,
            text=True,
            check=True
        )

    except subprocess.CalledProcessError as e:
        print(f"Error executing curl: {e}")
    
    return result



def download_package_file(minio_base_url, bucket_name, package_s3_key, local_file_path="/tmp/downloaded_package_from_s3"):
    """
    Downloads a package file from a MinIO bucket.

    Args:
        minio_base_url (str): Base URL of MinIO server (e.g., "http://localhost:9000").
        bucket_name (str): The name of the MinIO bucket.
        package_s3_key (str): The S3 key (object name) of the file.
        local_file_path (str, optional): Local path to save the downloaded file.

    Returns:
        str: The path of the downloaded file.
    """
    try:
        print(f"Connecting to MinIO at {minio_base_url}...\n")

        # Initialize MinIO client using boto3
        s3_client = boto3.client(
            's3',
            endpoint_url=minio_base_url,  # Use the passed MinIO URL
            aws_access_key_id="IAB1NY18WjiqGmAMvhj8",
            aws_secret_access_key="g7T5SB0INNVi5XqM5bpVrRDiWGQHgyX3Q1niH9Yj"
        )

        print(f"Downloading '{package_s3_key}' from bucket '{bucket_name}'...\n")

        # Download the file
        s3_client.download_file(bucket_name, package_s3_key, local_file_path)

        print(f"Download complete! File saved to {local_file_path}/{package_s3_key}\n")
        return local_file_path

    except NoCredentialsError:
        print("ERROR: Invalid MinIO credentials. Check Access Key & Secret Key.\n")
        sys.exit(1)

    except ClientError as e:
        print(f"ERROR: Failed to download file from MinIO: {e}\n")
        sys.exit(1)

def build_package_service_endpoint_url(base_url, id=None):
    if id is None:
        build_destination_url = f"{base_url}/{Type}/Package/FromJenkins"
    else:
        build_destination_url = f"{base_url}/{Type}/Package/{id}/FromJenkins"

    return build_destination_url


def main(PackageMetadata, PackageContentS3Key, Email, BaseUrl, Type):
    print(f"Start building ... \n")
    print(f"Current Pipeline type: {Type}")

    base_urls = get_base_urls(BaseUrl)
    package_service_base_url = base_urls[0]
    minio_base_url = base_urls[1]
    print(f"Base-URLs: {base_urls}\n")
    
    # Parse JSON
    try:
        parsed_data = json.loads(PackageMetadata)
    except json.JSONDecodeError:
        print("Invalid JSON input")
        sys.exit(1)    
    
    # Check http_method POST/PUR
    package_id = parsed_data.get("ID")
    if package_id is None:
        destination_url = build_package_service_endpoint_url(package_service_base_url)
        http_mode = "POST"
    else:
        destination_url = build_package_service_endpoint_url(package_service_base_url, package_id)
        http_mode = "PUT"

    match PackageContentS3Key:
        case str() if PackageContentS3Key:
            # Checks if it's a non-empty string, download file and append to curl command form data object
            download_package_file(minio_base_url, BUCKET_NAME, PackageContentS3Key)
            curl_command = build_form_data(parsed_data, destination_url, http_mode, f"/tmp/{PackageContentS3Key}")
        case _:
            # Default case (None or empty string)
            print(f"PackageContentS3Key is not provided. Skipping Download.")
            curl_command = build_form_data(parsed_data, destination_url, http_mode)     

    print(f"Sending curl command: {curl_command} ...\n")
    response = sending_curl_command(curl_command)

    if response.returncode == 0:  # Check if curl was successful
        print("Pipeline completed successfully\n")
        print(f"Response: {response.stdout}")
        sys.exit(0)  # Exit with success
        
    print("Curl command failed!\n")
    print(f"Error: {response.stderr}")
    sys.exit(1)  # Exit with failure
    

if __name__ == "__main__":
    PackageMetadata = sys.argv[1]
    PackageContentS3Key = sys.argv[2]
    Email = sys.argv[3]
    BaseUrl = sys.argv[4]
    Type = sys.argv[5]
    
    main(PackageMetadata, PackageContentS3Key, Email, BaseUrl, Type)

