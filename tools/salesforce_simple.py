"""
Salesforce File Download Tool for watsonx Orchestrate

This tool retrieves files from Salesforce and returns them as bytes for download.This is test
"""

import logging
import requests
from requests.exceptions import RequestException, Timeout, HTTPError
from pydantic import BaseModel, Field
from typing import Literal, Optional

# Tool decorator - will be provided by watsonx orchestrate at runtime
try:
    from ibm_watsonx_orchestrate.agent_builder.tools import tool
    from ibm_watsonx_orchestrate.agent_builder.connections import ConnectionType
    from ibm_watsonx_orchestrate.run import connections
except ImportError:
    # Fallback for local testing
    def tool(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    
    class ConnectionType:
        OAUTH2_AUTH_CODE = "oauth2_auth_code"

# Configure logging
logger = logging.getLogger(__name__)

# Salesforce connection configuration
SALESFORCE_APP_ID = 'salesforce-wxo'
SALESFORCE_API_VERSION = 'v58.0'
REQUEST_TIMEOUT = 60


class SalesforceFileInput(BaseModel):
    """Input model for Salesforce file download"""
    file_id: str = Field(
        default="069NS00000VB26HYAT",
        description="The Salesforce file ID. Supports ContentDocument (069), ContentVersion (068), or Attachment (00P) IDs"
    )


@tool(
    expected_credentials=[
        {"app_id": SALESFORCE_APP_ID, "type": ConnectionType.OAUTH2_AUTH_CODE}
    ]
)
def salesforce_download_file(input_data: SalesforceFileInput) -> bytes:
    """
    Download a file from Salesforce and return it as bytes.
    
    This tool retrieves files from Salesforce using the file ID. It supports three types of IDs:
    - ContentDocument ID (starts with 069) - Automatically retrieves the latest version
    - ContentVersion ID (starts with 068) - Downloads specific version
    - Attachment ID (starts with 00P) - Downloads attachment
    
    Use this tool when a user requests:
    - To download a file from Salesforce
    - To retrieve a document from Salesforce
    - To get file contents from Salesforce
    
    The tool returns the file as bytes that can be downloaded by the user.
    
    Args:
        input_data: Contains the file_id to download
        
    Returns:
        bytes: The file content as bytes for download
    """
    
    try:
        # Extract file ID from input
        file_id = input_data.file_id.strip()
        
        # Validate file ID
        if not file_id:
            raise ValueError("file_id cannot be empty")
        
        # Get OAuth2 credentials from the connection
        creds = connections.oauth2_auth_code(SALESFORCE_APP_ID)

        base_url = creds.url.rstrip('/')
        
        # Prepare authorization headers
        headers = {
            "Authorization": f"Bearer {creds.access_token}",
            "Accept": "*/*"
        }
        
        # Handle ContentDocument ID (069) - need to get ContentVersion ID first
        if file_id.startswith('069'):
            logger.info(f"ContentDocument ID detected: {file_id}")
            
            # Query for the latest ContentVersion
            query = (
                f"SELECT Id FROM ContentVersion "
                f"WHERE ContentDocumentId = '{file_id}' AND IsLatest = true"
            )
            query_url = f"{ base_url}/services/data/{SALESFORCE_API_VERSION}/query"
            
            query_response = requests.get(
                query_url, 
                headers=headers, 
                params={"q": query}, 
                timeout=REQUEST_TIMEOUT
            )
            query_response.raise_for_status()
            
            query_data = query_response.json()
            
            if not query_data.get('records'):
                raise ValueError(
                    f"No file found with ContentDocument ID: {file_id}"
                )
            
            # Get the ContentVersion ID
            content_version_id = query_data['records'][0]['Id']
            file_id = content_version_id
            logger.info(f"Found ContentVersion ID: {file_id}")
            
            # Set download URL for ContentVersion
            download_url = (
                f"{base_url}/services/data/{SALESFORCE_API_VERSION}/"
                f"sobjects/ContentVersion/{file_id}/VersionData"
            )
        
        # Handle ContentVersion ID (068)
        elif file_id.startswith('068'):
            logger.info(f"ContentVersion ID detected: {file_id}")
            download_url = (
                f"{base_url}/services/data/{SALESFORCE_API_VERSION}/"
                f"sobjects/ContentVersion/{file_id}/VersionData"
            )
        
        # Handle Attachment ID (00P)
        elif file_id.startswith('00P'):
            logger.info(f"Attachment ID detected: {file_id}")
            download_url = (
                f"{base_url}/services/data/{SALESFORCE_API_VERSION}/"
                f"sobjects/Attachment/{file_id}/Body"
            )
        
        else:
            raise ValueError(
                f"Unknown Salesforce ID format: {file_id}. "
                f"Expected ContentDocument (069), ContentVersion (068), "
                f"or Attachment (00P)"
            )
        
        # Download the file with streaming for large files
        logger.info(f"Downloading file from: {download_url}")
        
        response = requests.get(
            download_url,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            stream=True
        )
        
        # Raise exception for HTTP errors
        response.raise_for_status()
        
        # Get file content as bytes
        file_content = response.content
        file_size = len(file_content)
        
        logger.info(f"Successfully retrieved file ({file_size} bytes)")
        
        # Return bytes for download
        return file_content
        
    except Exception as e:
        # Return error message as bytes (text file)
        error_msg = f"Error downloading file from Salesforce: {str(e)}"
        logger.error(error_msg)
        return error_msg.encode('utf-8')


# Example usage for testing
if __name__ == "__main__":
    # Test the function locally
    test_input = SalesforceFileInput(
        file_id="069NS00000VB26HYAT"
    )
    
    # Note: This will fail without proper Salesforce credentials
    # Set environment variables for local testing:
    # export SALESFORCE_WXO_URL="https://your-instance.salesforce.com"
    # export SALESFORCE_WXO_ACCESS_TOKEN="your_access_token"
    
    print("Testing Salesforce file download...")
    print(f"File ID: {test_input.file_id}")