"""
JSON Parsing Tool Module

This module provides robust JSON extraction and parsing capabilities for the ESGAI system,
specifically designed to handle LLM responses that may contain JSON data in various formats.
It includes error handling and validation for reliable data extraction.

Key Responsibilities:
- Extract JSON from LLM responses with various formatting
- Validate required keys in parsed JSON objects
- Provide error handling and fallback mechanisms
- Support different JSON embedding formats (code blocks, raw JSON)

Dependencies:
- Python standard library: re, json

Related Files:
- agentic.py: Primary consumer for parsing manager agent responses
- Used for extracting sub-questions and company names from LLM outputs
- Enables structured data flow in the agentic workflow

External Dependencies:
- None (uses only Python standard library)
"""

import re
import json
from typing import Dict, List, Optional, Tuple, Any, Union

def parse_json_from_model_response(
    raw_response: str, 
    required_keys: Optional[List[str]] = None
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Parse JSON from various LLM response formats with robust error handling.
    
    This function is essential for extracting structured data from LLM responses,
    particularly for parsing manager agent outputs that contain sub-questions
    and company names in JSON format. Handles multiple JSON embedding formats.
    
    Args:
        raw_response (str): The raw text response from the LLM
        required_keys (Optional[List[str]]): List of keys that must be present in the JSON
            Example: ["list_of_sub_questions", "company_names"]
            
    Returns:
        Tuple[Optional[Dict[str, Any]], Optional[str]]: 
            - First element: Parsed JSON object if successful, None if failed
            - Second element: Error message if parsing failed, None if successful
            
    Supported JSON Formats:
        1. JSON in code blocks: ```json {...} ```
        2. JSON in code blocks without language: ``` {...} ```
        3. Raw JSON embedded in text: text {...} text
        4. Plain JSON string
        
    Workflow:
        1. Attempt to extract JSON from code blocks (```json or ```)
        2. Fall back to regex pattern matching for braces
        3. Parse extracted JSON string
        4. Validate required keys if specified
        5. Return parsed data or error message
        
    Related Files:
        - agentic.py: Primary consumer in manager() function
        - Uses parsed data for sub-question generation and company identification
        - Enables fallback behavior when JSON parsing fails
        
    Example Usage:
        ```python
        required_keys = ["list_of_sub_questions", "company_names"]
        parsed_data, error = parse_json_from_model_response(llm_response, required_keys)
        
        if error:
            # Handle parsing error with fallback
            use_default_questions()
        else:
            # Use parsed data
            sub_questions = parsed_data["list_of_sub_questions"]
            companies = parsed_data["company_names"]
        ```
    """
    # print(f"Raw model output: {raw_response}")
    
    try:
        # First, try to find JSON in code blocks (```json or ```)
        # This handles the most common LLM response format
        json_matches = re.findall(r'```(?:json)?(.*?)```', raw_response, re.DOTALL)
        if json_matches:
            cleaned_json = json_matches[0].strip()
        else:
            # If no code blocks, try to extract JSON based on braces
            # This handles cases where JSON is embedded in plain text
            json_pattern = re.compile(r'({.*})', re.DOTALL)
            match = json_pattern.search(raw_response)
            if match:
                cleaned_json = match.group(1).strip()
            else:
                # Fall back to the original content if we can't identify JSON pattern
                # This is a last resort attempt to parse the entire response as JSON
                cleaned_json = raw_response
                print("fall back to the original content could not find json pattern")
    
        # print(f"Extracted JSON: {cleaned_json}")
        print("extracted manager response json")
        
        # Parse the JSON response using Python's json library
        parsed_json = json.loads(cleaned_json)
        
        # Verify the expected keys are present if required_keys provided
        # This validation ensures the LLM provided all necessary fields
        if required_keys and not all(key in parsed_json for key in required_keys):
            missing_keys = [key for key in required_keys if key not in parsed_json]
            raise ValueError(f"Missing required keys in JSON response: {missing_keys}")
            
        # Return successful parsing result
        return parsed_json, None
        
    except (json.JSONDecodeError, ValueError) as e:
        print(f"Error parsing JSON: {e}")
        # Return the error so calling code can handle it
        # This enables fallback behavior in agentic.py when JSON parsing fails
        return None, str(e)