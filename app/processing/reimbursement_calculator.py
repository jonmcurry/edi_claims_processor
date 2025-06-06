# app/processing/reimbursement_calculator.py
"""
Performs reimbursement calculation on each claim line item: RVU * Units * Conversion Factor.
"""
from decimal import Decimal, ROUND_HALF_UP
import yaml
import os

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

    def _load_conversion_factor(self) -> Decimal:
        """Loads the reimbursement conversion factor from config."""
        try:
            config_data = self._load_config()
            factor_str = str(config_data['processing']['reimbursement_conversion_factor'])
            factor = Decimal(factor_str)
            logger.info(f"Reimbursement conversion factor loaded: {factor}")
            return factor
        except (ConfigError, KeyError, ValueError) as e:
            logger.error(f"Error loading reimbursement conversion factor: {e}. Using default of 0.0.", exc_info=True)
            # Fallback or raise critical error
            # For now, using 0.0 which will result in 0 reimbursement if factor is misconfigured.
            # raise ConfigError(f"Invalid or missing reimbursement conversion factor: {e}") from e
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
            # units = 1 # Or handle as error / return 0
            if units <=0:
                 return Decimal("0.00"), "INVALID_UNITS"


        rvu_details = self.rvu_cache.get_rvu_details(cpt_code)
        
        if rvu_details and 'rvu_value' in rvu_details: # Ensure 'rvu_value' is the correct key in your CSV/cache
            try:
                rvu_value_str = str(rvu_details['rvu_value'])
                rvu = Decimal(rvu_value_str)
                
                # Ensure units is treated as Decimal for precision with conversion_factor
                effective_units = Decimal(str(units)) if units is not None else Decimal('1')

                calculated_amount = (rvu * effective_units * self.conversion_factor)
                # Standard rounding to 2 decimal places for currency
                reimbursement = calculated_amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                
                logger.debug(f"Reimbursement for CPT {cpt_code}: RVU={rvu}, Units={effective_units}, Factor={self.conversion_factor} -> Calculated={reimbursement}")
                return reimbursement, "CALCULATED_OK"
            except (TypeError, ValueError) as e:
                logger.error(f"Error converting RVU value or units for CPT {cpt_code}: RVU Detail='{rvu_details.get('rvu_value')}', Units='{units}'. Error: {e}", exc_info=True)
                return Decimal("0.00"), "RVU_CONVERSION_ERROR"
        else:
            logger.warning(f"RVU value not found for CPT code: {cpt_code}. Cannot calculate RVU-based reimbursement.")
            # Placeholder: Could implement other logic e.g. % of charge, or just return 0
            # For now, return 0 if RVU not found.
            return Decimal("0.00"), "RVU_NOT_FOUND"

    def process_claim_reimbursement(self, claim_orm_object: any): # Type hint with StagingClaim if possible
        """
        Iterates through claim line items and calculates reimbursement for each.
        Updates the line items with the calculated amount (if a field exists for it).
        This method would typically be called after validation and ML prediction.

        Args:
            claim_orm_object: The SQLAlchemy StagingClaim object with its line items.
        """
        if not hasattr(claim_orm_object, 'cms1500_line_items'):
            logger.error(f"Claim object for ID {getattr(claim_orm_object, 'claim_id', 'Unknown')} does not have 'cms1500_line_items'. Cannot calculate reimbursement.")
            return

        total_claim_reimbursement = Decimal("0.00")
        logger.debug(f"Calculating reimbursement for claim: {claim_orm_object.claim_id}")

        for line_item in claim_orm_object.cms1500_line_items:
            try:
                # Ensure required fields are present
                if not line_item.cpt_code:
                    logger.warning(f"Line {line_item.line_number} for claim {claim_orm_object.claim_id} missing CPT code. Skipping reimbursement calculation.")
                    # Optionally set a specific status or reimbursement amount for this line
                    # line_item.estimated_reimbursement = Decimal("0.00")
                    # line_item.reimbursement_status = "CPT_MISSING"
                    continue

                units = line_item.units if line_item.units is not None else 1 # Default to 1 unit if None
                
                reimb_amount, status = self.calculate_line_item_reimbursement(
                    cpt_code=line_item.cpt_code,
                    units=units,
                    line_charge=line_item.line_charge_amount # Passed for context, not used in current RVU formula
                )
                
                # Add new attributes to line_item ORM object if your model supports them
                # For example, if StagingCMS1500LineItem has these fields:
                # line_item.estimated_reimbursement_amount = reimb_amount
                # line_item.reimbursement_calculation_status = status
                # logger.info(f"  Line {line_item.line_number} (CPT {line_item.cpt_code}): Estimated Reimbursement = {reimb_amount}, Status = {status}")

                # For now, let's assume we just log it or store it in a temporary structure
                # If you want to persist this, the ORM model needs fields for it.
                # This example doesn't modify the line_item ORM directly with reimbursement.
                # It would be better to store it on the claim or a related results table.

                total_claim_reimbursement += reimb_amount
            except Exception as e:
                logger.error(f"Error calculating reimbursement for line {line_item.line_number} (CPT {line_item.cpt_code}) on claim {claim_orm_object.claim_id}: {e}", exc_info=True)
                # line_item.reimbursement_calculation_status = "ERROR"

        # Store total estimated reimbursement on the claim object if it has such a field
        # claim_orm_object.total_estimated_reimbursement = total_claim_reimbursement
        logger.info(f"Total estimated reimbursement for claim {claim_orm_object.claim_id}: {total_claim_reimbursement}")
        
        # This method primarily calculates. Persisting the results would be handled by the caller
        # (e.g., claims_processor.py updating the StagingClaim ORM object).


if __name__ == '__main__':
    from app.utils.logging_config import setup_logging, set_correlation_id
    from app.database.connection_manager import init_database_connections, dispose_engines
    
    setup_logging()
    set_correlation_id("REIMB_CALC_TEST")

    # This test now assumes that the database is accessible and dbo.RvuData is populated.
    # The setup.py script is responsible for populating this data.
    try:
        init_database_connections()

        calculator = ReimbursementCalculator()
        logger.info(f"Using Conversion Factor: {calculator.conversion_factor}")

        # Test cases
        test_lines = [
            {"cpt": "99213", "units": 1, "charge": Decimal("150.00")},
            {"cpt": "99214", "units": 2, "charge": Decimal("300.00")},
            {"cpt": "G0008", "units": 1, "charge": Decimal("50.00")},
            {"cpt": "XXXXX", "units": 1, "charge": Decimal("100.00")}, # RVU not found
            {"cpt": "99213", "units": 0, "charge": Decimal("100.00")}, # Invalid units
            {"cpt": "", "units": 1, "charge": Decimal("100.00")},      # Missing CPT
        ]

        for line in test_lines:
            amount, status = calculator.calculate_line_item_reimbursement(line["cpt"], line["units"], line["charge"])
            logger.info(f"CPT: {line['cpt']}, Units: {line['units']} -> Reimbursement: {amount}, Status: {status}")
        
        # Example of processing a mock claim object
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