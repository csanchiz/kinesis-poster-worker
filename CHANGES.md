# Changes Made to Upgrade to Python 3 and boto3

## Summary
This repository has been upgraded to use Python 3 and boto3 (the AWS SDK for Python). The original functionality remains the same, but the code now uses modern Python practices and the latest AWS SDK.

## Detailed Changes

### Python 3 Compatibility
- Changed `xrange()` to `range()`
- Updated string handling for Python 3 (added decoding of binary data)
- Fixed string formatting and print statements
- Updated imports to be Python 3 compatible

### boto3 Integration
- Replaced all boto (v2) API calls with boto3 equivalents
- Updated parameter names to match boto3 requirements (e.g., `StreamName` instead of `stream_name`)
- Updated exception handling to use boto3's exception classes
- Improved client initialization and resource management

### Script Updates
- Modified shell scripts to use `python3` instead of `python`
- Made shell scripts executable with `chmod +x`

### Documentation
- Updated README.md with Python 3 and boto3 installation instructions
- Updated credential configuration instructions to include all modern methods
- Improved formatting and readability

## How to Use
The usage remains the same as before, just use `python3` instead of `python`:

```
# Create/use a stream
python3 poster.py my-first-stream

# Process records from the stream
python3 worker.py my-first-stream
```

## Requirements
- Python 3.6+
- boto3 library (`pip install boto3`)
- AWS credentials configured via AWS CLI, environment variables, or credentials file
