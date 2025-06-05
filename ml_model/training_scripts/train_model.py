# ml_model/training_scripts/train_model.py
"""
Production-ready script for training or retraining the ML model for filter prediction.
This script incorporates better configuration management, reproducibility,
and structured model artifact saving.
"""
import os
import yaml
import pandas as pd
import joblib
import json
import argparse
import uuid
from datetime import datetime
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier # Example model
from sklearn.metrics import classification_report, accuracy_score
from typing import Optional
# from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelEncoder
# from sklearn.compose import ColumnTransformer
# from sklearn.pipeline import Pipeline

from app.utils.logging_config import get_logger, setup_logging, set_correlation_id, get_correlation_id
from app.utils.error_handler import ConfigError, MLModelError

# Setup logging if running this script standalone
if __name__ == '__main__': # Guard to prevent setup_logging if imported
    setup_logging()

logger = get_logger('ml_model.train_model')

# --- Configuration Constants ---
DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
PREPROCESSOR_FILENAME = "preprocessor.pkl"
MODEL_METADATA_FILENAME = "model_metadata.json"

def load_app_config(config_path: str) -> dict:
    """Loads the main application configuration."""
    if not os.path.exists(config_path):
        logger.error(f"Application config file not found: {config_path}")
        raise ConfigError(f"Config file not found: {config_path}")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    logger.info(f"Loaded application configuration from: {config_path}")
    return config

def load_and_preprocess_data(data_path: str, ml_config: dict) -> tuple:
    """
    Loads and preprocesses training data based on configuration.
    """
    cid = get_correlation_id()
    logger.info(f"[{cid}] Loading training data from: {data_path}")
    if not os.path.exists(data_path):
        logger.error(f"[{cid}] Training data file not found: {data_path}")
        raise FileNotFoundError(f"Training data not found: {data_path}")

    df = pd.read_csv(data_path)
    logger.info(f"[{cid}] Loaded {len(df)} records for training.")

    feature_config = ml_config.get('feature_engineering', {})
    required_features = feature_config.get('numerical_features', []) + \
                        feature_config.get('categorical_features', [])
    target_column = feature_config.get('target_column', 'target_filter_id')
    
    # --- Feature Engineering & Preprocessing ---
    # This section MUST align with the preprocessing in `ml_predictor.py`
    # Example: Fill missing numerical values with median, categorical with mode.
    for col in feature_config.get('numerical_features', []):
        if col in df.columns:
            df[col].fillna(df[col].median(), inplace=True)
        else:
            logger.warning(f"[{cid}] Numerical feature '{col}' not found in data. Filling with 0.")
            df[col] = 0

    for col in feature_config.get('categorical_features', []):
        if col in df.columns:
            df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else 'UNKNOWN', inplace=True)
        else:
            logger.warning(f"[{cid}] Categorical feature '{col}' not found in data. Filling with 'UNKNOWN'.")
            df[col] = 'UNKNOWN'

    # Placeholder for actual preprocessing steps (e.g., scaling, encoding)
    # preprocessor = None
    # if feature_config.get('use_scaler', False) or feature_config.get('use_encoder', False):
    #     # Initialize ColumnTransformer with StandardScaler for numerical and OneHotEncoder for categorical
    #     # preprocessor = ColumnTransformer(...)
    #     # X_processed = preprocessor.fit_transform(df[required_features])
    #     # logger.info(f"[{cid}] Applied preprocessing (scaling/encoding).")
    #     pass
    # else:
    #     X_processed = df[required_features]

    # For this example, we'll stick to the original simple feature selection
    df.fillna({ # Default fillna based on original script's logic if features not in config
        'total_charge_amount': 0.0,
        'patient_age': df['patient_age'].median() if 'patient_age' in df and not df['patient_age'].isnull().all() else 30,
        'num_diagnoses': 0,
        'num_line_items': 0
    }, inplace=True)

    # Define features (X) and target (y)
    missing_features = [f for f in required_features if f not in df.columns]
    if missing_features:
        msg = f"[{cid}] Missing required feature columns in training data: {missing_features}"
        logger.error(msg)
        raise ValueError(msg)
    if target_column not in df.columns:
        msg = f"[{cid}] Missing target column '{target_column}' in training data."
        logger.error(msg)
        raise ValueError(msg)

    X = df[required_features]
    y = df[target_column]
    
    # Example: Label Encoding for target if it's categorical
    # label_encoder = None
    # if y.dtype == 'object' or feature_config.get('encode_target', False):
    #     label_encoder = LabelEncoder()
    #     y = label_encoder.fit_transform(y)
    #     logger.info(f"[{cid}] Target classes after encoding: {label_encoder.classes_}")
    #     # TODO: Save this label_encoder alongside the model to decode predictions later.

    logger.info(f"[{cid}] Features shape: {X.shape}, Target shape: {y.shape}")
    logger.info(f"[{cid}] Sample features:\n{X.head()}")
    logger.info(f"[{cid}] Sample target:\n{y.head()}")
    
    # Return preprocessor and label_encoder if used, so they can be saved.
    return X, y #, preprocessor, label_encoder

def train_evaluate_model(X_train, y_train, X_test, y_test, model_params: dict):
    """
    Trains an ML model and evaluates it.
    Returns the trained model and its evaluation metrics.
    """
    cid = get_correlation_id()
    logger.info(f"[{cid}] Training model with parameters: {model_params}")
    
    # Set random seed for reproducibility
    random_state = model_params.get('random_state', 42)
    np.random.seed(random_state)

    # Example model: RandomForestClassifier
    model = RandomForestClassifier(
        n_estimators=model_params.get('n_estimators', 100),
        random_state=random_state,
        class_weight=model_params.get('class_weight', 'balanced'),
        max_depth=model_params.get('max_depth'),
        min_samples_split=model_params.get('min_samples_split', 2),
        min_samples_leaf=model_params.get('min_samples_leaf', 1)
    )
    
    # If using a pipeline with preprocessing:
    # pipeline = Pipeline([
    #     ('preprocessor', preprocessor_object_from_load_data), # if preprocessor is not part of X_train
    #     ('classifier', model)
    # ])
    # pipeline.fit(X_train, y_train)
    # y_pred = pipeline.predict(X_test)
    # trained_model_object = pipeline

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    trained_model_object = model
    
    accuracy = accuracy_score(y_test, y_pred)
    report_dict = classification_report(y_test, y_pred, output_dict=True)
    
    metrics = {
        "accuracy": accuracy,
        "classification_report": report_dict,
        "f1_score_weighted": report_dict.get("weighted avg", {}).get("f1-score", 0.0) # Example
    }
    
    logger.info(f"[{cid}] Model Accuracy: {accuracy:.4f}")
    logger.info(f"[{cid}] Classification Report (F1-Weighted): {metrics['f1_score_weighted']:.4f}")
    
    return trained_model_object, metrics

def save_model_artifacts(model, preprocessor, label_encoder, metrics: dict, model_config: dict, output_dir: str, model_filename: str):
    """Saves the trained model, preprocessor, label encoder, and metadata to disk."""
    cid = get_correlation_id()
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"[{cid}] Created output directory: {output_dir}")
    
    model_path = os.path.join(output_dir, model_filename)
    preprocessor_path = os.path.join(output_dir, PREPROCESSOR_FILENAME) if preprocessor else None
    # label_encoder_path = os.path.join(output_dir, "label_encoder.pkl") if label_encoder else None # If used

    try:
        joblib.dump(model, model_path)
        logger.info(f"[{cid}] Trained model saved successfully to: {model_path}")
        
        if preprocessor and preprocessor_path:
            joblib.dump(preprocessor, preprocessor_path)
            logger.info(f"[{cid}] Preprocessor saved successfully to: {preprocessor_path}")
        
        # if label_encoder and label_encoder_path:
        #     joblib.dump(label_encoder, label_encoder_path)
        #     logger.info(f"[{cid}] Label encoder saved successfully to: {label_encoder_path}")

        # Save metadata
        model_version = model_config.get('version', datetime.now().strftime("%Y%m%d%H%M%S"))
        metadata = {
            "model_version": model_version,
            "model_filename": model_filename,
            "preprocessor_filename": PREPROCESSOR_FILENAME if preprocessor_path else None,
            # "label_encoder_filename": "label_encoder.pkl" if label_encoder_path else None,
            "training_timestamp": datetime.now().isoformat(),
            "training_data_path": model_config.get('training_data_path'),
            "model_parameters": model_config.get('model_params'),
            "feature_config": model_config.get('feature_engineering'),
            "evaluation_metrics": metrics,
            "correlation_id": cid
        }
        metadata_path = os.path.join(output_dir, MODEL_METADATA_FILENAME)
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=4)
        logger.info(f"[{cid}] Model metadata saved successfully to: {metadata_path}")

        # Placeholder for MLflow (or other experiment tracking)
        # import mlflow
        # mlflow.log_params(model_config.get('model_params'))
        # mlflow.log_metrics(metrics)
        # mlflow.sklearn.log_model(model, "model", registered_model_name=model_config.get('model_name'))
        # if preprocessor: mlflow.log_artifact(preprocessor_path, "preprocessor")

    except Exception as e:
        logger.error(f"[{cid}] Error saving model artifacts to {output_dir}: {e}", exc_info=True)
        raise MLModelError(f"Failed to save model artifacts: {e}")


def main_training_pipeline(config_path: str, training_data_override: Optional[str] = None, output_dir_override: Optional[str] = None):
    """Orchestrates the model training pipeline."""
    run_id = str(uuid.uuid4())[:8]
    cid = set_correlation_id(f"ML_TRAINING_{run_id}")
    logger.info(f"[{cid}] Starting ML model training pipeline...")

    try:
        app_config = load_app_config(config_path)
        ml_config = app_config.get('ml_model', {})
        if not ml_config:
            raise ConfigError("ml_model section not found in configuration.")

        # Override paths if provided
        training_data_path = training_data_override or ml_config.get('training_data_path', 'data/ml_training_data/claims_with_filters.csv')
        model_output_dir_template = output_dir_override or ml_config.get('model_output_dir', 'ml_model/versions')
        
        # Create a versioned output directory
        model_version_name = ml_config.get('model_name', 'filter_predictor') + "_" + datetime.now().strftime("%Y%m%d_%H%M%S")
        model_output_dir = os.path.join(model_output_dir_template, model_version_name)
        
        model_filename = ml_config.get('model_filename', 'model.pkl')
        
        # Store paths in ml_config for metadata saving
        ml_config['training_data_path'] = training_data_path
        ml_config['version'] = model_version_name # Store the generated version

        logger.info(f"[{cid}] Training Data Path: {training_data_path}")
        logger.info(f"[{cid}] Model Output Directory: {model_output_dir}")
        logger.info(f"[{cid}] Model Filename: {model_filename}")

        # --- Start MLflow Run (Example) ---
        # with mlflow.start_run(run_name=f"training_run_{cid}"):
        #     mlflow.log_param("config_path", config_path)
        #     mlflow.log_param("training_data_path", training_data_path)
        #     mlflow.log_param("model_output_dir", model_output_dir)
        #     mlflow.set_tag("correlation_id", cid)

        # Load and preprocess data
        # X, y, preprocessor, label_encoder = load_and_preprocess_data(training_data_path, ml_config) # If preprocessor/encoder used
        X, y = load_and_preprocess_data(training_data_path, ml_config)
        preprocessor = None # Placeholder
        label_encoder = None # Placeholder
        
        # Split data
        test_size = ml_config.get('test_split_ratio', 0.2)
        random_state_split = ml_config.get('random_state', 42)
        
        # Stratify if target is categorical and has multiple classes
        stratify_target = y if len(y.unique()) > 1 and y.dtype != 'float' else None
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state_split, stratify=stratify_target)
        logger.info(f"[{cid}] Data split: Train size={len(X_train)}, Test size={len(X_test)}")

        # Train and evaluate model
        model_params = ml_config.get('model_params', {})
        trained_model, metrics = train_evaluate_model(X_train, y_train, X_test, y_test, model_params)
        
        # Save model and artifacts
        save_model_artifacts(trained_model, preprocessor, label_encoder, metrics, ml_config, model_output_dir, model_filename)
        
        logger.info(f"[{cid}] ML model training pipeline completed successfully. Model version '{model_version_name}' saved to '{model_output_dir}'.")

    except FileNotFoundError as fnf_err:
        logger.error(f"[{cid}] File not found during training: {fnf_err}. Please check paths.")
        raise MLModelError(f"File not found: {fnf_err}", error_code="TRAINING_FILE_NOT_FOUND") from fnf_err
    except ConfigError as conf_err:
        logger.error(f"[{cid}] Configuration error: {conf_err}")
        raise
    except ValueError as val_err:
        logger.error(f"[{cid}] Value error during training data processing or model training: {val_err}", exc_info=True)
        raise MLModelError(f"Data or parameter error: {val_err}", error_code="TRAINING_VALUE_ERROR") from val_err
    except Exception as e:
        logger.critical(f"[{cid}] An unexpected error occurred during the ML training pipeline: {e}", exc_info=True)
        raise MLModelError(f"Unexpected training error: {e}", error_code="TRAINING_UNEXPECTED_ERROR") from e
    finally:
        # Ensure correlation ID is reset if it was changed within this function for sub-tasks
        set_correlation_id(cid.split('_')[-1] if '_' in cid else cid) # Attempt to restore original part of CID

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="ML Model Training Script")
    parser.add_argument("--config", type=str, default=DEFAULT_CONFIG_PATH, help="Path to the main application config.yaml file.")
    parser.add_argument("--data_path", type=str, help="Override training data CSV path from config.")
    parser.add_argument("--output_dir", type=str, help="Override base model output directory from config (version subdir will be created).")
    parser.add_argument("--create_dummy_data", action="store_true", help="Create dummy training data if main data path not found.")
    args = parser.parse_args()

    config_to_use = load_app_config(args.config)
    data_path_from_config = config_to_use.get('ml_model', {}).get('training_data_path', 'data/ml_training_data/claims_with_filters.csv')
    effective_data_path = args.data_path or data_path_from_config

    if args.create_dummy_data and not os.path.exists(effective_data_path):
        logger.warning(f"Training data {effective_data_path} not found. Creating dummy data for test as requested.")
        dummy_dir = os.path.dirname(effective_data_path)
        if not os.path.exists(dummy_dir):
            os.makedirs(dummy_dir, exist_ok=True)
        
        dummy_df = pd.DataFrame({
            'claim_id': [f'C{i}' for i in range(100)],
            'total_charge_amount': [100.0 * (i % 5 + 1) for i in range(100)],
            'patient_age': [20 + (i % 50) for i in range(100)],
            'num_diagnoses': [(i % 4) + 1 for i in range(100)],
            'num_line_items': [(i % 3) + 1 for i in range(100)],
            'target_filter_id': [i % 3 for i in range(100)] # Example: ternary target 0, 1, or 2
        })
        # Add some categorical features if configured
        ml_conf = config_to_use.get('ml_model', {})
        cat_features = ml_conf.get('feature_engineering', {}).get('categorical_features', [])
        for cat_feat in cat_features:
            if cat_feat not in dummy_df.columns: # Avoid overwriting common numerical like patient_age
                 dummy_df[cat_feat] = [f'CAT_VAL_{(i % 3)}' for i in range(100)]


        dummy_df.to_csv(effective_data_path, index=False)
        logger.info(f"Dummy training data created at {effective_data_path}")

    main_training_pipeline(
        config_path=args.config,
        training_data_override=args.data_path, # This will be effective_data_path if --data_path is not set.
        output_dir_override=args.output_dir
    )