import re
import json

def parse_json_from_model_response(raw_response, required_keys=None):
    """
    Parse JSON from various model response formats.
    
    Args:
        raw_response (str): The raw response from the model
        required_keys (list): List of keys that must be present in the JSON
        
    Returns:
        dict: Parsed JSON object or default values if parsing fails
    """
    # print(f"Raw model output: {raw_response}")
    
    try:
        # First, try to find JSON in code blocks
        json_matches = re.findall(r'```(?:json)?(.*?)```', raw_response, re.DOTALL)
        if json_matches:
            cleaned_json = json_matches[0].strip()
        else:
            # If no code blocks, try to extract JSON based on braces
            json_pattern = re.compile(r'({.*})', re.DOTALL)
            match = json_pattern.search(raw_response)
            if match:
                cleaned_json = match.group(1).strip()
            else:
                # Fall back to the original content if we can't identify JSON pattern
                cleaned_json = raw_response
                print("fall back to the original content could not find json pattern")
    
        # print(f"Extracted JSON: {cleaned_json}")
        print("extracted manager response json")
        parsed_json = json.loads(cleaned_json) # Parse the JSON response
        
        # Verify the expected keys are present if required_keys provided
        if required_keys and not all(key in parsed_json for key in required_keys):
            missing_keys = [key for key in required_keys if key not in parsed_json]
            raise ValueError(f"Missing required keys in JSON response: {missing_keys}")
            
        return parsed_json, None
        
    except (json.JSONDecodeError, ValueError) as e:
        print(f"Error parsing JSON: {e}")
        # Return the error so calling code can handle it
        return None, str(e)