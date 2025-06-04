# app/utils/security.py
"""
Utilities for PII/PHI data protection, encryption/decryption, and other security measures.
Placeholder for now, as actual implementation requires careful design and robust libraries.
"""
from app.utils.logging_config import get_logger
from app.utils.error_handler import AppException, ConfigError
# from cryptography.fernet import Fernet # Example encryption library
import os
import yaml
import hashlib # For hashing, not encryption of PII

logger = get_logger('app.security')

# --- Configuration Loading ---
def _load_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    if not os.path.exists(config_path):
        raise ConfigError("config.yaml not found for security configuration.")
    with open(config_path, 'rt') as f:
        config = yaml.safe_load(f)
    if 'security' not in config:
        logger.warning("Security section missing in config.yaml. Using placeholder values.")
        return {} # Return empty dict if security section is missing
    return config.get('security', {})

CONFIG = _load_config()

# --- Encryption/Decryption (Placeholder) ---
# IMPORTANT: This is a VERY basic placeholder. For real PII/PHI, use a robust,
# well-vetted library like `cryptography` and manage keys securely (e.g., via a vault).
# The key management is the hardest part.

_ENCRYPTION_KEY = CONFIG.get('pii_phi_encryption_key_placeholder', 'default_insecure_key_change_me!')
if _ENCRYPTION_KEY == 'default_insecure_key_change_me!' or len(_ENCRYPTION_KEY) < 32: # Example check
    logger.critical("INSECURE OR DEFAULT ENCRYPTION KEY DETECTED. THIS IS NOT SAFE FOR PRODUCTION.")
    # In a real app, you might prevent startup or use a proper key management system.

# For a real Fernet implementation:
# try:
#     _FERNET_INSTANCE = Fernet(_ENCRYPTION_KEY.encode()) # Key must be 32 url-safe base64-encoded bytes
# except Exception as e:
#     logger.critical(f"Failed to initialize Fernet for encryption (key issue?): {e}", exc_info=True)
#     _FERNET_INSTANCE = None

def encrypt_data(data: str) -> str:
    """
    Encrypts a string. Placeholder implementation.
    Replace with robust encryption.
    """
    if not data:
        return data
    # if not _FERNET_INSTANCE:
    #     logger.error("Encryption service not available (Fernet not initialized). Returning raw data.")
    #     return data # Or raise an error
    # try:
    #     return _FERNET_INSTANCE.encrypt(data.encode()).decode()
    # except Exception as e:
    #     logger.error(f"Encryption failed: {e}", exc_info=True)
    #     raise AppException("Data encryption failed.", error_code="ENCRYPTION_ERROR") from e
    logger.warning("Using placeholder encryption. THIS IS NOT SECURE.")
    return f"encrypted({data[::-1]})" # Example: reverses string

def decrypt_data(encrypted_data: str) -> str:
    """
    Decrypts a string. Placeholder implementation.
    Replace with robust decryption.
    """
    if not encrypted_data or not encrypted_data.startswith("encrypted("):
        return encrypted_data # Or handle as an error if it should have been encrypted
    # if not _FERNET_INSTANCE:
    #     logger.error("Decryption service not available (Fernet not initialized). Returning raw data.")
    #     return encrypted_data
    # try:
    #     return _FERNET_INSTANCE.decrypt(encrypted_data.encode()).decode()
    # except Exception as e: # InvalidToken, etc.
    #     logger.error(f"Decryption failed: {e}", exc_info=True)
    #     raise AppException("Data decryption failed. Data may be corrupt or key incorrect.", error_code="DECRYPTION_ERROR") from e
    logger.warning("Using placeholder decryption. THIS IS NOT SECURE.")
    if encrypted_data.startswith("encrypted(") and encrypted_data.endswith(")"):
        return encrypted_data[10:-1][::-1] # Reverse the placeholder
    return encrypted_data # Should not happen if properly encrypted

# --- Data Masking / Anonymization (Placeholders) ---

def mask_phi_field(field_value: str, field_type: str = "generic") -> str:
    """
    Masks a PII/PHI field.
    Example: mask SSN, DOB, names.
    This is a very basic placeholder. Real PII masking is complex.
    """
    if not field_value:
        return field_value

    if field_type == "ssn" and len(field_value) >= 9:
        return f"***-**-{field_value[-4:]}"
    elif field_type == "dob_year": # Only show year
        parts = field_value.split('-') # Assuming YYYY-MM-DD
        if len(parts) == 3:
            return f"{parts[0]}-XX-XX"
        return "XXXX-XX-XX"
    elif field_type == "name":
        return f"{field_value[0]}***{field_value[-1] if len(field_value) > 1 else '*'}"
    elif field_type == "account_number" and len(field_value) > 4:
        return f"********{field_value[-4:]}"
    # Generic masking
    return "*" * len(field_value) if len(field_value) > 3 else field_value


def hash_data(data: str, salt: str = None) -> str:
    """
    Hashes data (e.g., for identifiers if you don't want to store them raw but need to check for duplicates).
    Uses SHA256. A fixed salt can be provided from config for consistency if needed,
    or a per-item salt stored separately for stronger security (though that makes lookups harder).
    For PII, prefer encryption if you need to recover the original value. Hashing is one-way.
    """
    if not data:
        return None
    
    effective_salt = salt or CONFIG.get('hashing_salt_placeholder', 'default_insecure_salt')
    if effective_salt == 'default_insecure_salt':
        logger.warning("Using default insecure salt for hashing. THIS IS NOT RECOMMENDED FOR PRODUCTION.")

    salted_data = effective_salt + data
    return hashlib.sha256(salted_data.encode()).hexdigest()


# --- Audit Logging for Security Events ---
# (Leverages logging_config.py and the 'audit' logger)
audit_logger = get_logger('audit.security')

def log_security_event(event_description: str, user_id: str = "system", details: dict = None):
    """Logs a security-relevant event to the audit trail."""
    log_message = f"SecurityEvent: User='{user_id}', Event='{event_description}'"
    audit_logger.info(log_message, extra={'security_details': details or {}})


if __name__ == '__main__':
    from app.utils.logging_config import setup_logging, set_correlation_id
    setup_logging()
    set_correlation_id("SECURITY_TEST")

    logger.info("--- Security Utilities Test ---")

    # Encryption/Decryption Test (Placeholder)
    original_text = "Patient Name: John Doe, DOB: 1980-01-15"
    logger.info(f"Original: {original_text}")
    
    encrypted_text = encrypt_data(original_text)
    logger.info(f"Encrypted (Placeholder): {encrypted_text}")
    
    decrypted_text = decrypt_data(encrypted_text)
    logger.info(f"Decrypted (Placeholder): {decrypted_text}")

    if original_text == decrypted_text:
        logger.info("Placeholder Encryption/Decryption test PASSED.")
    else:
        logger.error("Placeholder Encryption/Decryption test FAILED.")

    # Masking Test
    ssn = "123456789"
    dob = "1990-05-20"
    name = "Alice Wonderland"
    acc_num = "ACC1234567890"
    
    logger.info(f"Original SSN: {ssn}, Masked: {mask_phi_field(ssn, 'ssn')}")
    logger.info(f"Original DOB: {dob}, Masked (Year Only): {mask_phi_field(dob, 'dob_year')}")
    logger.info(f"Original Name: {name}, Masked: {mask_phi_field(name, 'name')}")
    logger.info(f"Original Account: {acc_num}, Masked: {mask_phi_field(acc_num, 'account_number')}")

    # Hashing Test
    data_to_hash = "unique_patient_identifier_123"
    hashed_value = hash_data(data_to_hash)
    logger.info(f"Data: {data_to_hash}, Hashed: {hashed_value}")
    
    hashed_value_again = hash_data(data_to_hash) # Should be the same with same default salt
    logger.info(f"Data: {data_to_hash}, Hashed Again: {hashed_value_again}")
    if hashed_value == hashed_value_again:
        logger.info("Hashing consistency test PASSED.")
    else:
        logger.error("Hashing consistency test FAILED.")

    # Audit Log Test
    log_security_event("User login attempt failed", user_id="test_user", details={"reason": "Invalid password"})
    log_security_event("PII data accessed", user_id="data_analyst_01", details={"record_id": "Claim789", "fields_accessed": ["patient_name", "diagnosis_code"]})
    logger.info("Check audit.log for security event entries.")