# app/processing/ml_predictor.py
"""
Interfaces with the trained Machine Learning model for filter prediction.
"""
import os
import yaml
import joblib # Using joblib as it's common for scikit-learn models
# import pickle # Alternative for model loading if not joblib

from app.utils.logging_config import get_logger
from app.utils.error_handler import MLModelError, ConfigError, handle_exception
# from app.database.models.postgres_models import StagingClaim # If needed for feature extraction

logger = get_logger('app.processing.ml_predictor')

class MLPredictor:
    """
    Handles loading the ML model and making predictions.
    """
    _model = None
    _model_path = None
    _model_version = "N/A" # Could be loaded from model metadata if available

    def __init__(self):
        if MLPredictor._model is None: # Ensure model is loaded only once (singleton-like behavior)
            self._load_model()

    def _load_config(self):
        """Loads ML model configuration from config.yaml."""
        config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
        if not os.path.exists(config_path):
            raise ConfigError("config.yaml not found for ML predictor configuration.")
        with open(config_path, 'rt') as f:
            config = yaml.safe_load(f)
        if 'ml_model' not in config or not config['ml_model'].get('path'):
            raise ConfigError("ML model path (ml_model.path) not configured in config.yaml.")
        return config['ml_model']

    def _load_model(self):
        """Loads the serialized ML model from the path specified in config."""
        try:
            ml_config = self._load_config()
            MLPredictor._model_path = ml_config['path']
            
            if not os.path.exists(MLPredictor._model_path):
                msg = f"ML model file not found at path: {MLPredictor._model_path}"
                logger.error(msg)
                raise MLModelError(msg, model_name=os.path.basename(MLPredictor._model_path))

            logger.info(f"Loading ML model from: {MLPredictor._model_path}")
            # Assuming the model was saved with joblib
            MLPredictor._model = joblib.load(MLPredictor._model_path)
            # Or with pickle:
            # with open(MLPredictor._model_path, 'rb') as f:
            #     MLPredictor._model = pickle.load(f)
            
            # Optionally, load model version/metadata if stored with the model
            # e.g., if model is a dict: MLPredictor._model_version = MLPredictor._model.get('version', 'N/A')
            # For now, assume _model is the predictor itself.
            logger.info(f"ML model loaded successfully: {type(MLPredictor._model)}")

        except ConfigError as ce:
            logger.error(f"Configuration error loading ML model: {ce.message}")
            MLPredictor._model = None # Ensure model is None if loading fails
            raise # Re-raise ConfigError
        except FileNotFoundError as fnfe:
            logger.error(f"ML model file not found: {fnfe}")
            MLPredictor._model = None
            raise MLModelError(f"ML model file not found: {MLPredictor._model_path}") from fnfe
        except Exception as e:
            logger.error(f"Error loading ML model from {MLPredictor._model_path}: {e}", exc_info=True)
            MLPredictor._model = None
            raise MLModelError(f"Failed to load ML model: {e}", model_name=os.path.basename(MLPredictor._model_path or "Unknown")) from e

    def _preprocess_claim_data_for_model(self, claim_data: dict) -> list: # Or pd.DataFrame, np.array
        """
        Preprocesses raw claim data (dict or StagingClaim object) into the feature format
        expected by the ML model. This is highly dependent on your model's training.
        
        Args:
            claim_data (dict): A dictionary representing the claim.
                               Could include fields like 'total_charge_amount', 'patient_age',
                               number of diagnoses, number of procedures, specific CPT/ICD codes (e.g. one-hot encoded), etc.
        
        Returns:
            list or np.array or pd.DataFrame: The feature vector for the model.
        """
        # --- This is a CRITICAL and HIGHLY CUSTOMIZED part ---
        # The features must match exactly what the model was trained on.
        # Example features (these are just illustrative):
        # - Total charge amount
        # - Patient age
        # - Number of diagnosis codes
        # - Number of line items/procedures
        # - Presence of certain CPT/ICD codes (e.g., one-hot encoded or as counts)
        # - Payer type (numeric representation)
        # - Provider specialty (numeric representation)

        # For this placeholder, let's assume the model expects a simple list of numerical features:
        # [total_charge, patient_age, num_diagnoses, num_line_items]
        
        try:
            # Ensure values are numeric and handle missing data appropriately (e.g., imputation)
            # This should mirror the preprocessing done during model training.
            total_charge = float(claim_data.get('total_charge_amount', 0.0) or 0.0)
            patient_age = int(claim_data.get('patient_age', 30) or 30) # Default age if missing
            num_diagnoses = len(claim_data.get('diagnoses', []))
            num_line_items = len(claim_data.get('line_items', []))

            feature_vector = [total_charge, patient_age, num_diagnoses, num_line_items]
            logger.debug(f"Prepared feature vector for claim {claim_data.get('claim_id', 'N/A')}: {feature_vector}")
            return [feature_vector] # Model's predict usually expects a 2D array-like (list of samples)
        
        except Exception as e:
            msg = f"Error preprocessing data for ML model for claim {claim_data.get('claim_id', 'N/A')}: {e}"
            logger.error(msg, exc_info=True)
            raise MLModelError(msg, model_name=os.path.basename(self._model_path or "Unknown")) from e


    def predict_filters(self, claim_data: dict) -> tuple[list, float] : # Returns (predicted_filters_list, prediction_probability_if_binary_or_max_prob)
        """
        Predicts filter numbers/categories for a given claim.

        Args:
            claim_data (dict): A dictionary representing the claim, structured to allow
                               feature extraction by `_preprocess_claim_data_for_model`.

        Returns:
            tuple: A list of predicted filter IDs/names and the probability/confidence score.
                   The structure depends on whether it's multi-label, multi-class, or binary.
                   For simplicity, let's assume it predicts a primary filter and its probability.
        """
        if MLPredictor._model is None:
            msg = "ML model is not loaded. Cannot make predictions."
            logger.error(msg)
            raise MLModelError(msg)

        try:
            features = self._preprocess_claim_data_for_model(claim_data)
            
            # Make prediction
            # If model.predict_proba exists (common for classifiers)
            if hasattr(MLPredictor._model, 'predict_proba'):
                probabilities = MLPredictor._model.predict_proba(features)[0] # Probabilities for each class for the first sample
                predicted_class_index = probabilities.argmax()
                max_probability = probabilities[predicted_class_index]
                
                # Assume classes correspond to filter IDs/names. This mapping needs to be defined.
                # For placeholder, let's say class indices map directly to filter names like "FILTER_A", "FILTER_B"
                # You'd need a list or dict: class_to_filter_mapping = {0: "FILTER_A", 1: "FILTER_B", ...}
                # This mapping should be consistent with how the model was trained.
                predicted_filter = f"ML_FILTER_{predicted_class_index}" # Placeholder
                
                logger.info(f"Claim {claim_data.get('claim_id', 'N/A')}: Predicted filter '{predicted_filter}' with probability {max_probability:.4f}")
                return [predicted_filter], float(max_probability)

            # If model only has .predict (e.g., some regressors or simpler classifiers)
            else:
                prediction = MLPredictor._model.predict(features)[0]
                # `prediction` could be a class label or a numerical value.
                # Adapt this based on your model's output.
                # If it's a class label, convert it to your filter ID.
                predicted_filter = f"ML_FILTER_{prediction}" # Placeholder
                logger.info(f"Claim {claim_data.get('claim_id', 'N/A')}: Predicted filter '{predicted_filter}' (no probability available).")
                return [predicted_filter], 1.0 # Default probability if not available

        except Exception as e:
            msg = f"Error during ML prediction for claim {claim_data.get('claim_id', 'N/A')}: {e}"
            logger.error(msg, exc_info=True)
            # Re-raise as MLModelError or return a default/error indicator
            raise MLModelError(msg, model_name=os.path.basename(self._model_path or "Unknown")) from e
            # return ["ERROR_IN_PREDICTION"], 0.0

    def get_model_version(self) -> str:
        return self._model_version

    def get_model_path(self) -> str:
        return self._model_path


if __name__ == '__main__':
    from app.utils.logging_config import setup_logging, set_correlation_id
    setup_logging()
    set_correlation_id("ML_PREDICTOR_TEST")

    # --- Mocking a simple ML model for testing ---
    # In a real scenario, model.pkl would exist.
    # We'll create a dummy one here if it doesn't.
    
    # Create a dummy config.yaml if it doesn't exist for the test
    test_config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
    if not os.path.exists(test_config_path):
        os.makedirs(os.path.dirname(test_config_path), exist_ok=True)
        with open(test_config_path, 'w') as f:
            yaml.dump({
                'ml_model': {'path': 'ml_model/dummy_model.pkl'},
                'logging': {'level': 'DEBUG', 'log_dir': 'logs/', 
                            'app_log_file': 'logs/app_test.log', 'audit_log_file': 'logs/audit_test.log'}
            }, f)

    dummy_model_dir = "ml_model"
    dummy_model_path = os.path.join(dummy_model_dir, "dummy_model.pkl")
    os.makedirs(dummy_model_dir, exist_ok=True)

    if not os.path.exists(dummy_model_path):
        from sklearn.linear_model import LogisticRegression
        import numpy as np
        # Create a very simple dummy model that expects 4 features
        # and predicts one of two classes.
        X_dummy = np.array([[100, 20, 1, 1], [2000, 60, 5, 3], [50, 80, 2, 1], [800, 40, 3, 2]])
        y_dummy = np.array([0, 1, 0, 1])
        dummy_model = LogisticRegression()
        dummy_model.fit(X_dummy, y_dummy)
        joblib.dump(dummy_model, dummy_model_path)
        logger.info(f"Dummy ML model created at {dummy_model_path}")
    # --- End Mocking ---

    try:
        predictor = MLPredictor() # Loads the model via __init__
        logger.info(f"ML Predictor initialized. Model path: {predictor.get_model_path()}")

        # Example claim data (must match structure expected by _preprocess_claim_data_for_model)
        sample_claim_1 = {
            "claim_id": "CLAIM_ML_001",
            "total_charge_amount": Decimal("150.75"),
            "patient_age": 45,
            "diagnoses": [{"icd_code": "R51"}, {"icd_code": "M54.5"}], # 2 diagnoses
            "line_items": [{"cpt_code": "99213", "units": 1}] # 1 line item
        }
        
        predicted_filters, probability = predictor.predict_filters(sample_claim_1)
        logger.info(f"Prediction for {sample_claim_1['claim_id']}: Filters={predicted_filters}, Probability={probability:.4f}")

        sample_claim_2 = {
            "claim_id": "CLAIM_ML_002",
            "total_charge_amount": Decimal("2200.00"),
            "patient_age": 68,
            "diagnoses": [{"icd_code": "I10"}, {"icd_code": "E11.9"}, {"icd_code": "N18.3"}], # 3 diagnoses
            "line_items": [ # 3 line items
                {"cpt_code": "99204", "units": 1},
                {"cpt_code": "80053", "units": 1},
                {"cpt_code": "71045", "units": 1}
            ]
        }
        predicted_filters_2, probability_2 = predictor.predict_filters(sample_claim_2)
        logger.info(f"Prediction for {sample_claim_2['claim_id']}: Filters={predicted_filters_2}, Probability={probability_2:.4f}")

    except MLModelError as e:
        logger.error(f"ML Model Error during test: {e.message} (Code: {e.error_code})")
    except ConfigError as e:
        logger.error(f"Config Error during test: {e.message}")
    except Exception as e:
        logger.critical(f"Unexpected error in ML Predictor test: {e}", exc_info=True)
    finally:
        # Clean up dummy model and config if created by this test
        # if os.path.exists(dummy_model_path) and "dummy_model.pkl" in dummy_model_path:
        #     os.remove(dummy_model_path)
        # if os.path.exists(test_config_path) and "config.yaml" in test_config_path: # Be careful
            # os.remove(test_config_path) # Or restore original if backed up
        logger.info("ML Predictor test finished.")