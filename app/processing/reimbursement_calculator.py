# app/processing/reimbursement_calculator.py
"""
Performs reimbursement calculation on each claim line item: RVU * Units * Conversion Factor.
"""
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import yaml
import os
import re

from app.utils.logging_config import get_logger
from app.utils.caching import rvu_cache_instance # Singleton instance from caching.py
from app.utils.error_handler import AppException, ConfigError
# from app.database.models.postgres_models import StagingCMS1500LineItem # For type hinting if needed

logger = get_logger('app.processing.reimbursement_calculator')

class ReimbursementCalculator:
    """
    Calculates reimbursement amounts for claim line items.
    """
    def __init__(self):
        self.conversion_factor = self._load_conversion_factor()
        if rvu_cache_instance is None:
            msg = "RVU Cache is not available. Reimbursement calculations requiring RVUs will fail."
            logger.error(msg)
            # Depending on strictness, could raise an error here.
            # For now, allow initialization but methods will fail if RVUs are needed.
            # raise RuntimeError(msg) 
        self.rvu_cache = rvu_cache_instance

    def _load_config(self):
        """Loads configuration from config.yaml."""
        config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
        if not os.path.exists(config_path):
            raise ConfigError("config.yaml not found for reimbursement calculator.")
        with open(config_path, 'rt') as f:
            config = yaml.safe_load(f)
        if 'processing' not in config or 'reimbursement_conversion_factor' not in config['processing']:
            raise ConfigError("Reimbursement conversion factor not found in config.yaml (processing.reimbursement_conversion_factor).")
        return config

    def _resolve_config_value(self, value: any) -> str:
        """Resolves environment variables in config values."""
        if not isinstance(value, str):
            return str(value)
        
        pattern = r'\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]+))?\}'
        
        def replace_match(match):
            var_name, default_value = match.groups()
            return os.environ.get(var_name, default_value if default_value is not None else "")
        
        return re.sub(pattern, replace_match, value)

    def _load_conversion_factor(self) -> Decimal:
        """Loads the reimbursement conversion factor from config."""
        try:
            config_data = self._load_config()
            factor_config_value = config_data['processing']['reimbursement_conversion_factor']
            factor_str = self._resolve_config_value(factor_config_value)
            factor = Decimal(factor_str)
            logger.info(f"Reimbursement conversion factor loaded: {factor}")
            return factor
        except (ConfigError, KeyError, ValueError, InvalidOperation) as e:
            logger.error(f"Error loading or parsing reimbursement conversion factor: {e}. Using default of 0.0.", exc_info=True)
            return Decimal("0.0")


    def calculate_line_item_reimbursement(self, cpt_code: str, units: int, line_charge: Decimal) -> tuple[Decimal, str]:
        """
        Calculates the estimated reimbursement for a single claim line item.
        Formula: RVU * Units * Conversion Factor.
        If RVU is not found, reimbursement might be based on a percentage of charge or other rules (not implemented here).

        Args:
            cpt_code (str): The CPT/HCPCS code for the service.
            units (int): The number of units for the service.
            line_charge (Decimal): The original charge amount for the line item.

        Returns:
            Tuple[Decimal, str]: The calculated reimbursement amount and a status/note.
                                 Returns (Decimal('0.00'), "RVU not found") if RVU is missing.
        """
        if self.rvu_cache is None:
            logger.warning(f"RVU cache not available. Cannot calculate RVU-based reimbursement for CPT {cpt_code}.")
            return Decimal("0.00"), "RVU_CACHE_UNAVAILABLE"

        if not cpt_code:
            logger.warning("CPT code is missing. Cannot calculate reimbursement.")
            return Decimal("0.00"), "CPT_CODE_MISSING"
        
        if units is None or units <= 0:
            logger.warning(f"Invalid units ({units}) for CPT {cpt_code}. Assuming 1 unit for calculation if RVU found, or 0 reimbursement.")
            if units <=0:
                 return Decimal("0.00"), "INVALID_UNITS"


        rvu_details = self.rvu_cache.get_rvu_details(cpt_code)
        
        if rvu_details and 'rvu_value' in rvu_details and rvu_details.get('rvu_value') is not None:
            try:
                rvu_value_str = str(rvu_details['rvu_value'])
                rvu = Decimal(rvu_value_str)
                
                effective_units = Decimal(str(units)) if units is not None else Decimal('1')

                calculated_amount = (rvu * effective_units * self.conversion_factor)
                reimbursement = calculated_amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                
                logger.debug(f"Reimbursement for CPT {cpt_code}: RVU={rvu}, Units={effective_units}, Factor={self.conversion_factor} -> Calculated={reimbursement}")
                return reimbursement, "CALCULATED_OK"
            except (TypeError, ValueError, InvalidOperation) as e:
                logger.error(f"Error converting RVU value or units for CPT {cpt_code}: RVU Detail='{rvu_details.get('rvu_value')}', Units='{units}'. Error: {e}", exc_info=True)
                return Decimal("0.00"), "RVU_CONVERSION_ERROR"
        else:
            logger.warning(f"RVU value not found for CPT code: {cpt_code}. Cannot calculate RVU-based reimbursement.")
            return Decimal("0.00"), "RVU_NOT_FOUND"

    def process_claim_reimbursement(self, claim_orm_object: any): # Type hint with StagingClaim if possible
        """
        Iterates through claim line items and calculates reimbursement for each.
        Updates the line items with the calculated amount (if a field exists for it).
        """
        if not hasattr(claim_orm_object, 'cms1500_line_items'):
            logger.error(f"Claim object for ID {getattr(claim_orm_object, 'claim_id', 'Unknown')} does not have 'cms1500_line_items'. Cannot calculate reimbursement.")
            return

        total_claim_reimbursement = Decimal("0.00")
        logger.debug(f"Calculating reimbursement for claim: {claim_orm_object.claim_id}")

        for line_item in claim_orm_object.cms1500_line_items:
            try:
                if not line_item.cpt_code:
                    logger.warning(f"Line {line_item.line_number} for claim {claim_orm_object.claim_id} missing CPT code. Skipping reimbursement calculation.")
                    continue

                units = line_item.units if line_item.units is not None else 1
                
                reimb_amount, status = self.calculate_line_item_reimbursement(
                    cpt_code=line_item.cpt_code,
                    units=units,
                    line_charge=line_item.line_charge_amount
                )
                
                if hasattr(line_item, 'estimated_reimbursement_amount'):
                    line_item.estimated_reimbursement_amount = reimb_amount
                if hasattr(line_item, 'reimbursement_calculation_status'):
                    line_item.reimbursement_calculation_status = status

                total_claim_reimbursement += reimb_amount
            except Exception as e:
                logger.error(f"Error calculating reimbursement for line {line_item.line_number} (CPT {line_item.cpt_code}) on claim {claim_orm_object.claim_id}: {e}", exc_info=True)

        if hasattr(claim_orm_object, 'total_estimated_reimbursement'):
            claim_orm_object.total_estimated_reimbursement = total_claim_reimbursement
        logger.info(f"Total estimated reimbursement for claim {claim_orm_object.claim_id}: {total_claim_reimbursement}")


if __name__ == '__main__':
    from app.utils.logging_config import setup_logging, set_correlation_id
    from app.database.connection_manager import init_database_connections, dispose_engines
    
    setup_logging()
    set_correlation_id("REIMB_CALC_TEST")

    try:
        init_database_connections()

        calculator = ReimbursementCalculator()
        logger.info(f"Using Conversion Factor: {calculator.conversion_factor}")

        test_lines = [
            {"cpt": "99213", "units": 1, "charge": Decimal("150.00")},
            {"cpt": "99214", "units": 2, "charge": Decimal("300.00")},
            {"cpt": "G0008", "units": 1, "charge": Decimal("50.00")},
            {"cpt": "XXXXX", "units": 1, "charge": Decimal("100.00")},
            {"cpt": "99213", "units": 0, "charge": Decimal("100.00")},
            {"cpt": "", "units": 1, "charge": Decimal("100.00")},
        ]

        for line in test_lines:
            amount, status = calculator.calculate_line_item_reimbursement(line["cpt"], line["units"], line["charge"])
            logger.info(f"CPT: {line['cpt']}, Units: {line['units']} -> Reimbursement: {amount}, Status: {status}")
        
        class MockLineItem:
            def __init__(self, line_number, cpt_code, units, line_charge_amount):
                self.line_number = line_number
                self.cpt_code = cpt_code
                self.units = units
                self.line_charge_amount = line_charge_amount

        class MockClaim:
            def __init__(self, claim_id):
                self.claim_id = claim_id
                self.cms1500_line_items = []

        mock_claim_obj = MockClaim("TEST_REIMB_001")
        mock_claim_obj.cms1500_line_items.append(MockLineItem(1, "99213", 1, Decimal("150.00")))
        mock_claim_obj.cms1500_line_items.append(MockLineItem(2, "G0008", 2, Decimal("60.00")))
        
        calculator.process_claim_reimbursement(mock_claim_obj) 

    except Exception as e:
        logger.error(f"Reimbursement calculator test failed: {e}", exc_info=True)
    finally:
        dispose_engines()
    
    logger.info("Reimbursement calculator test finished.")
