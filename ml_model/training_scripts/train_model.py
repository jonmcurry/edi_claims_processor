# ml_model/training_scripts/train_model.py
"""
Placeholder script for training or retraining the ML model for filter prediction.
This script would typically:
1. Load training data (e.g., historical claims and their actual filters).
2. Perform feature engineering and preprocessing (must match ml_predictor.py).
3. Train an ML model (e.g., scikit-learn classifier).
4. Evaluate the model's performance.
5. Serialize and save the trained model (e.g., to model.pkl).
"""
import os
import pandas as pd
import joblib
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier # Example model
from sklearn.metrics import classification_report, accuracy_score
# from sklearn.preprocessing import StandardScaler, OneHotEncoder # Example preprocessors
# from sklearn.compose import ColumnTransformer # Example for complex preprocessing
# from sklearn.pipeline import Pipeline # Example pipeline

from app.utils.logging_config import get_logger, setup_logging, set_correlation_id

# Setup logging if running this script standalone
if __name__ == '__main__': # Guard to prevent setup_logging if imported
    setup_logging()

logger = get_logger('ml_model.train_model')

# --- Configuration (could also come from a dedicated ML config file) ---
MODEL_OUTPUT_DIR = "ml_model"
MODEL_FILENAME = "model.pkl" # Must match path in config.yaml: ml_model.path
TRAINING_DATA_PATH = "data/ml_training_data/claims_with_filters.csv" # Example path

def load_and_preprocess_data(data_path: str) -> tuple: # pd.DataFrame, pd.Series:
    """
    Loads and preprocesses training data.
    This is highly dependent on your data and model.
    """
    logger.info(f"Loading training data from: {data_path}")
    if not os.path.exists(data_path):
        logger.error(f"Training data file not found: {data_path}")
        raise FileNotFoundError(f"Training data not found: {data_path}")

    df = pd.read_csv(data_path)
    logger.info(f"Loaded {len(df)} records for training.")

    # --- Placeholder Feature Engineering & Preprocessing ---
    # Example: Assume 'target_filter_id' is the column to predict.
    # Assume features are: 'total_charge', 'patient_age', 'num_diagnoses', 'num_line_items'
    # This MUST align with _preprocess_claim_data_for_model in ml_predictor.py

    # Basic preprocessing:
    df.fillna({
        'total_charge_amount': 0.0,
        'patient_age': df['patient_age'].median() if 'patient_age' in df and not df['patient_age'].isnull().all() else 30,
        'num_diagnoses': 0,
        'num_line_items': 0
    }, inplace=True)
    
    # Define features (X) and target (y)
    # Adjust column names based on your actual CSV
    required_features = ['total_charge_amount', 'patient_age', 'num_diagnoses', 'num_line_items']
    target_column = 'target_filter_id' # Example target column name

    missing_features = [f for f in required_features if f not in df.columns]
    if missing_features:
        msg = f"Missing required feature columns in training data: {missing_features}"
        logger.error(msg)
        raise ValueError(msg)
    if target_column not in df.columns:
        msg = f"Missing target column '{target_column}' in training data."
        logger.error(msg)
        raise ValueError(msg)

    X = df[required_features]
    y = df[target_column] 
    
    # Example: If target_filter_id is categorical, you might need to encode it
    # from sklearn.preprocessing import LabelEncoder
    # label_encoder = LabelEncoder()
    # y = label_encoder.fit_transform(y)
    # logger.info(f"Target classes: {label_encoder.classes_}")
    # TODO: Save this label_encoder alongside the model if used, to decode predictions later.

    logger.info(f"Features shape: {X.shape}, Target shape: {y.shape}")
    logger.info(f"Sample features:\n{X.head()}")
    logger.info(f"Sample target:\n{y.head()}")
    
    return X, y #, label_encoder if used

def train_evaluate_model(X_train, y_train, X_test, y_test):
    """
    Trains a model and evaluates it.
    Returns the trained model.
    """
    logger.info("Training Random Forest Classifier model...")
    # Example model: RandomForestClassifier
    model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
    
    # Create a pipeline if more complex preprocessing is needed per split
    # pipeline = Pipeline([
    #     ('scaler', StandardScaler()), # Example step
    #     ('classifier', model)
    # ])
    # pipeline.fit(X_train, y_train)
    # y_pred = pipeline.predict(X_test)
    # trained_model_object = pipeline

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    trained_model_object = model
    
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)
    
    logger.info(f"Model Accuracy: {accuracy:.4f}")
    logger.info("Classification Report:\n" + report)
    
    return trained_model_object # Return the actual model/pipeline

def save_model(model, output_dir: str, filename: str):
    """Saves the trained model to disk."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    model_path = os.path.join(output_dir, filename)
    try:
        joblib.dump(model, model_path)
        logger.info(f"Trained model saved successfully to: {model_path}")
    except Exception as e:
        logger.error(f"Error saving model to {model_path}: {e}", exc_info=True)
        raise

def main_training_pipeline():
    """Orchestrates the model training pipeline."""
    cid = set_correlation_id("ML_TRAINING_RUN")
    logger.info(f"[{cid}] Starting ML model training pipeline...")

    try:
        X, y = load_and_preprocess_data(TRAINING_DATA_PATH)
        
        # Split data into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y if len(y.unique()) > 1 else None)
        logger.info(f"Data split: Train size={len(X_train)}, Test size={len(X_test)}")

        trained_model = train_evaluate_model(X_train, y_train, X_test, y_test)
        
        save_model(trained_model, MODEL_OUTPUT_DIR, MODEL_FILENAME)
        
        logger.info(f"[{cid}] ML model training pipeline completed successfully.")

    except FileNotFoundError:
        logger.error(f"[{cid}] Training data not found. Cannot train model. Please place training data at: {TRAINING_DATA_PATH}")
    except ValueError as ve:
        logger.error(f"[{cid}] Value error during training data processing: {ve}")
    except Exception as e:
        logger.critical(f"[{cid}] An unexpected error occurred during the ML training pipeline: {e}", exc_info=True)
    finally:
        set_correlation_id(cid) # Restore just in case

if __name__ == '__main__':
    # Create dummy training data if it doesn't exist for a quick test run
    if not os.path.exists(TRAINING_DATA_PATH):
        logger.warning(f"Training data {TRAINING_DATA_PATH} not found. Creating dummy data for test.")
        os.makedirs(os.path.dirname(TRAINING_DATA_PATH), exist_ok=True)
        dummy_df = pd.DataFrame({
            'claim_id': [f'C{i}' for i in range(100)],
            'total_charge_amount': [100.0 * (i % 5 + 1) for i in range(100)],
            'patient_age': [20 + (i % 50) for i in range(100)],
            'num_diagnoses': [ (i % 4) + 1 for i in range(100)],
            'num_line_items': [ (i % 3) + 1 for i in range(100)],
            'target_filter_id': [ i % 2 for i in range(100)] # Example: binary target 0 or 1
        })
        dummy_df.to_csv(TRAINING_DATA_PATH, index=False)
        logger.info(f"Dummy training data created at {TRAINING_DATA_PATH}")

    main_training_pipeline()
