# app/processing/ml_predictor.py
"""
Production-ready ML predictor with async loading, version management, 
batch optimization, and performance monitoring.
"""
import os
import yaml
import joblib
import asyncio
import threading
import time
from typing import Dict, List, Tuple, Optional, Any, Union
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from decimal import Decimal
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from pathlib import Path
import hashlib
import json

from app.utils.logging_config import get_logger, get_correlation_id, set_correlation_id
from app.utils.error_handler import MLModelError, ConfigError, handle_exception
from app.utils.caching import get_from_in_memory_cache, set_in_memory_cache

logger = get_logger('app.processing.ml_predictor')

@dataclass
class ModelMetadata:
    """Metadata for ML model tracking."""
    version: str
    model_hash: str
    loaded_at: datetime
    file_path: str
    model_type: str
    feature_names: List[str] = field(default_factory=list)
    performance_metrics: Dict[str, float] = field(default_factory=dict)
    training_date: Optional[datetime] = None

@dataclass
class PredictionMetrics:
    """Metrics for monitoring prediction performance."""
    total_predictions: int = 0
    batch_predictions: int = 0
    avg_inference_time_ms: float = 0.0
    avg_batch_inference_time_ms: float = 0.0
    error_count: int = 0
    last_prediction_time: Optional[datetime] = None
    predictions_per_second: float = 0.0

class ModelVersionManager:
    """Manages model versions and loading strategies."""
    
    def __init__(self, model_dir: str):
        self.model_dir = Path(model_dir)
        self.available_versions: Dict[str, ModelMetadata] = {}
        self._scan_for_models()
    
    def _scan_for_models(self):
        """Scan directory for available model versions."""
        if not self.model_dir.exists():
            logger.warning(f"Model directory {self.model_dir} does not exist")
            return
            
        for model_file in self.model_dir.glob("*.pkl"):
            try:
                metadata = self._extract_metadata(model_file)
                self.available_versions[metadata.version] = metadata
                logger.debug(f"Found model version {metadata.version}: {model_file}")
            except Exception as e:
                logger.warning(f"Could not process model file {model_file}: {e}")
    
    def _extract_metadata(self, model_file: Path) -> ModelMetadata:
        """Extract metadata from model file."""
        # Calculate file hash for integrity checking
        with open(model_file, 'rb') as f:
            file_hash = hashlib.md5(f.read()).hexdigest()
        
        # Try to load metadata from companion file
        metadata_file = model_file.with_suffix('.json')
        if metadata_file.exists():
            with open(metadata_file, 'r') as f:
                metadata_dict = json.load(f)
                return ModelMetadata(
                    version=metadata_dict.get('version', model_file.stem),
                    model_hash=file_hash,
                    loaded_at=datetime.now(),
                    file_path=str(model_file),
                    model_type=metadata_dict.get('model_type', 'unknown'),
                    feature_names=metadata_dict.get('feature_names', []),
                    performance_metrics=metadata_dict.get('performance_metrics', {}),
                    training_date=datetime.fromisoformat(metadata_dict['training_date']) 
                        if metadata_dict.get('training_date') else None
                )
        
        # Fallback to basic metadata
        return ModelMetadata(
            version=model_file.stem,
            model_hash=file_hash,
            loaded_at=datetime.now(),
            file_path=str(model_file),
            model_type='sklearn',  # Default assumption
            feature_names=['total_charge_amount', 'patient_age', 'num_diagnoses', 'num_line_items']
        )
    
    def get_latest_version(self) -> Optional[str]:
        """Get the latest model version based on naming convention or metadata."""
        if not self.available_versions:
            return None
        
        # Sort by version (assuming semantic versioning or timestamp)
        sorted_versions = sorted(
            self.available_versions.keys(),
            key=lambda v: self.available_versions[v].training_date or datetime.min,
            reverse=True
        )
        return sorted_versions[0] if sorted_versions else None
    
    def get_model_metadata(self, version: str) -> Optional[ModelMetadata]:
        """Get metadata for specific model version."""
        return self.available_versions.get(version)

class AsyncModelLoader:
    """Handles asynchronous model loading with caching."""
    
    def __init__(self, version_manager: ModelVersionManager):
        self.version_manager = version_manager
        self._loaded_models: Dict[str, Any] = {}
        self._loading_futures: Dict[str, asyncio.Future] = {}
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="ModelLoader")
        self._lock = threading.Lock()
    
    async def load_model_async(self, version: str) -> Tuple[Any, ModelMetadata]:
        """Asynchronously load a model version."""
        # Check if model is already loaded
        if version in self._loaded_models:
            metadata = self.version_manager.get_model_metadata(version)
            return self._loaded_models[version], metadata
        
        # Check if loading is in progress
        with self._lock:
            if version in self._loading_futures:
                logger.debug(f"Model {version} is already being loaded, waiting...")
                return await self._loading_futures[version]
            
            # Start loading
            future = asyncio.create_task(self._load_model_task(version))
            self._loading_futures[version] = future
        
        try:
            model, metadata = await future
            with self._lock:
                self._loaded_models[version] = model
                if version in self._loading_futures:
                    del self._loading_futures[version]
            return model, metadata
        except Exception as e:
            with self._lock:
                if version in self._loading_futures:
                    del self._loading_futures[version]
            raise
    
    async def _load_model_task(self, version: str) -> Tuple[Any, ModelMetadata]:
        """Internal task for loading model."""
        metadata = self.version_manager.get_model_metadata(version)
        if not metadata:
            raise MLModelError(f"Model version {version} not found")
        
        logger.info(f"Loading model {version} from {metadata.file_path}")
        
        # Use thread executor for blocking I/O
        loop = asyncio.get_event_loop()
        model = await loop.run_in_executor(
            self._executor,
            joblib.load,
            metadata.file_path
        )
        
        logger.info(f"Successfully loaded model {version}")
        return model, metadata
    
    def preload_models(self, versions: List[str]):
        """Preload models synchronously (for startup)."""
        for version in versions:
            try:
                metadata = self.version_manager.get_model_metadata(version)
                if metadata:
                    model = joblib.load(metadata.file_path)
                    self._loaded_models[version] = model
                    logger.info(f"Preloaded model {version}")
            except Exception as e:
                logger.error(f"Failed to preload model {version}: {e}")
    
    def cleanup(self):
        """Cleanup resources."""
        self._executor.shutdown(wait=True)

class BatchPredictor:
    """Optimized batch prediction with numpy vectorization."""
    
    def __init__(self, model: Any, feature_names: List[str]):
        self.model = model
        self.feature_names = feature_names
        self.batch_size_threshold = 10  # Minimum batch size for optimization
    
    def predict_batch(self, claims_data: List[Dict]) -> Tuple[List[List[str]], List[float], float]:
        """
        Predict filters for a batch of claims with optimization.
        
        Returns:
            Tuple of (predictions, probabilities, inference_time_ms)
        """
        start_time = time.perf_counter()
        
        if len(claims_data) >= self.batch_size_threshold:
            return self._predict_batch_optimized(claims_data, start_time)
        else:
            return self._predict_batch_simple(claims_data, start_time)
    
    def _predict_batch_optimized(self, claims_data: List[Dict], start_time: float) -> Tuple[List[List[str]], List[float], float]:
        """Optimized batch prediction using vectorized operations."""
        try:
            # Vectorized feature extraction
            features_matrix = self._extract_features_vectorized(claims_data)
            
            # Batch prediction
            if hasattr(self.model, 'predict_proba'):
                probabilities = self.model.predict_proba(features_matrix)
                predicted_classes = np.argmax(probabilities, axis=1)
                max_probabilities = np.max(probabilities, axis=1)
            else:
                predicted_classes = self.model.predict(features_matrix)
                max_probabilities = np.ones(len(predicted_classes))
            
            # Convert to filter names
            predictions = []
            for class_idx in predicted_classes:
                predictions.append([f"ML_FILTER_{class_idx}"])
            
            inference_time_ms = (time.perf_counter() - start_time) * 1000
            
            logger.debug(f"Batch prediction for {len(claims_data)} claims completed in {inference_time_ms:.2f}ms")
            
            return predictions, max_probabilities.tolist(), inference_time_ms
            
        except Exception as e:
            logger.error(f"Error in optimized batch prediction: {e}")
            # Fallback to simple prediction
            return self._predict_batch_simple(claims_data, start_time)
    
    def _predict_batch_simple(self, claims_data: List[Dict], start_time: float) -> Tuple[List[List[str]], List[float], float]:
        """Simple batch prediction (one by one)."""
        predictions = []
        probabilities = []
        
        for claim_data in claims_data:
            try:
                features = self._extract_features_single(claim_data)
                
                if hasattr(self.model, 'predict_proba'):
                    proba = self.model.predict_proba([features])[0]
                    predicted_class = np.argmax(proba)
                    max_prob = np.max(proba)
                else:
                    predicted_class = self.model.predict([features])[0]
                    max_prob = 1.0
                
                predictions.append([f"ML_FILTER_{predicted_class}"])
                probabilities.append(float(max_prob))
                
            except Exception as e:
                logger.error(f"Error predicting for claim {claim_data.get('claim_id')}: {e}")
                predictions.append(["ERROR_PREDICTION"])
                probabilities.append(0.0)
        
        inference_time_ms = (time.perf_counter() - start_time) * 1000
        return predictions, probabilities, inference_time_ms
    
    def _extract_features_vectorized(self, claims_data: List[Dict]) -> np.ndarray:
        """Extract features using vectorized operations."""
        # Create pandas DataFrame for efficient vectorized operations
        df = pd.DataFrame(claims_data)
        
        # Extract features with proper handling of missing values
        features = []
        
        # Total charge amount
        total_charges = pd.to_numeric(df.get('total_charge_amount', 0), errors='coerce').fillna(0.0)
        features.append(total_charges.values)
        
        # Patient age
        patient_ages = pd.to_numeric(df.get('patient_age', 30), errors='coerce').fillna(30)
        features.append(patient_ages.values)
        
        # Number of diagnoses
        num_diagnoses = df.get('diagnoses', []).apply(lambda x: len(x) if isinstance(x, list) else 0)
        features.append(num_diagnoses.values)
        
        # Number of line items
        num_line_items = df.get('line_items', []).apply(lambda x: len(x) if isinstance(x, list) else 0)
        features.append(num_line_items.values)
        
        return np.column_stack(features)
    
    def _extract_features_single(self, claim_data: Dict) -> List[float]:
        """Extract features for a single claim."""
        total_charge = float(claim_data.get('total_charge_amount', 0.0) or 0.0)
        patient_age = int(claim_data.get('patient_age', 30) or 30)
        num_diagnoses = len(claim_data.get('diagnoses', []))
        num_line_items = len(claim_data.get('line_items', []))
        
        return [total_charge, patient_age, num_diagnoses, num_line_items]

class MLPredictor:
    """Production-ready ML predictor with comprehensive monitoring and optimization."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config = self._load_config(config_path)
        self.model_dir = self.config.get('model_directory', 'ml_model')
        self.current_version = None
        self.current_model = None
        self.current_metadata = None
        
        # Initialize components
        self.version_manager = ModelVersionManager(self.model_dir)
        self.model_loader = AsyncModelLoader(self.version_manager)
        self.batch_predictor = None
        
        # Performance monitoring
        self.metrics = PredictionMetrics()
        self.performance_window = timedelta(minutes=5)
        self.recent_predictions = []
        
        # Configuration
        self.batch_threshold = self.config.get('batch_threshold', 10)
        self.cache_predictions = self.config.get('cache_predictions', True)
        self.monitor_performance = self.config.get('monitor_performance', True)
        
        # Thread safety
        self._prediction_lock = threading.Lock()
        
        # Initialize with latest model if available
        self._initialize_default_model()
    
    def _load_config(self, config_path: Optional[str] = None) -> Dict:
        """Load ML predictor configuration."""
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.yaml')
        
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            return config.get('ml_model', {})
        except Exception as e:
            logger.warning(f"Could not load ML config: {e}. Using defaults.")
            return {}
    
    def _initialize_default_model(self):
        """Initialize with the latest available model synchronously."""
        latest_version = self.version_manager.get_latest_version()
        if latest_version:
            try:
                # Preload the latest model synchronously for immediate availability
                self.model_loader.preload_models([latest_version])
                self.current_version = latest_version
                self.current_model = self.model_loader._loaded_models.get(latest_version)
                self.current_metadata = self.version_manager.get_model_metadata(latest_version)
                
                if self.current_model and self.current_metadata:
                    self.batch_predictor = BatchPredictor(
                        self.current_model, 
                        self.current_metadata.feature_names
                    )
                    logger.info(f"Initialized with model version {latest_version}")
                else:
                    logger.warning("Model loaded but batch predictor could not be initialized")
            except Exception as e:
                logger.error(f"Failed to initialize default model: {e}")
    
    async def load_model_version(self, version: str) -> bool:
        """Load a specific model version asynchronously."""
        try:
            model, metadata = await self.model_loader.load_model_async(version)
            
            with self._prediction_lock:
                self.current_version = version
                self.current_model = model
                self.current_metadata = metadata
                self.batch_predictor = BatchPredictor(model, metadata.feature_names)
            
            logger.info(f"Successfully switched to model version {version}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load model version {version}: {e}")
            raise MLModelError(f"Failed to load model version {version}: {e}")
    
    def predict_filters(self, claim_data: Dict) -> Tuple[List[str], float]:
        """Predict filters for a single claim with caching."""
        return self.predict_filters_batch([claim_data])[0]
    
    def predict_filters_batch(self, claims_data: List[Dict]) -> List[Tuple[List[str], float]]:
        """Predict filters for multiple claims with optimization."""
        if not self.current_model or not self.batch_predictor:
            raise MLModelError("No model loaded. Cannot make predictions.")
        
        start_time = time.perf_counter()
        cid = get_correlation_id()
        
        try:
            # Check cache for batch predictions if enabled
            if self.cache_predictions and len(claims_data) == 1:
                cache_key = self._generate_cache_key(claims_data[0])
                cached_result = get_from_in_memory_cache(cache_key)
                if cached_result:
                    logger.debug(f"Cache hit for claim {claims_data[0].get('claim_id')}")
                    return [cached_result]
            
            # Perform batch prediction
            with self._prediction_lock:
                predictions, probabilities, inference_time_ms = self.batch_predictor.predict_batch(claims_data)
            
            # Update metrics
            self._update_metrics(len(claims_data), inference_time_ms, start_time)
            
            # Cache results if enabled
            if self.cache_predictions:
                for i, claim_data in enumerate(claims_data):
                    if i < len(predictions):
                        cache_key = self._generate_cache_key(claim_data)
                        set_in_memory_cache(cache_key, (predictions[i], probabilities[i]))
            
            # Combine results
            results = list(zip(predictions, probabilities))
            
            logger.info(f"[{cid}] Batch prediction completed for {len(claims_data)} claims in {inference_time_ms:.2f}ms")
            
            return results
            
        except Exception as e:
            self.metrics.error_count += 1
            logger.error(f"[{cid}] Error in batch prediction: {e}", exc_info=True)
            raise MLModelError(f"Prediction failed: {e}") from e
    
    def _generate_cache_key(self, claim_data: Dict) -> str:
        """Generate cache key for claim data."""
        # Use claim features for cache key
        features_str = f"{claim_data.get('total_charge_amount', 0)}_{claim_data.get('patient_age', 30)}_{len(claim_data.get('diagnoses', []))}_{len(claim_data.get('line_items', []))}"
        return f"ml_pred_{self.current_version}_{hashlib.md5(features_str.encode()).hexdigest()[:8]}"
    
    def _update_metrics(self, batch_size: int, inference_time_ms: float, start_time: float):
        """Update performance metrics."""
        if not self.monitor_performance:
            return
        
        current_time = datetime.now()
        
        # Update counters
        self.metrics.total_predictions += batch_size
        if batch_size > 1:
            self.metrics.batch_predictions += 1
        
        # Update timing metrics
        self.metrics.last_prediction_time = current_time
        
        # Update rolling averages
        if batch_size == 1:
            if self.metrics.avg_inference_time_ms == 0:
                self.metrics.avg_inference_time_ms = inference_time_ms
            else:
                self.metrics.avg_inference_time_ms = (
                    self.metrics.avg_inference_time_ms * 0.9 + inference_time_ms * 0.1
                )
        else:
            avg_per_claim = inference_time_ms / batch_size
            if self.metrics.avg_batch_inference_time_ms == 0:
                self.metrics.avg_batch_inference_time_ms = avg_per_claim
            else:
                self.metrics.avg_batch_inference_time_ms = (
                    self.metrics.avg_batch_inference_time_ms * 0.9 + avg_per_claim * 0.1
                )
        
        # Track recent predictions for throughput calculation
        self.recent_predictions.append((current_time, batch_size))
        cutoff_time = current_time - self.performance_window
        self.recent_predictions = [
            (ts, count) for ts, count in self.recent_predictions if ts > cutoff_time
        ]
        
        # Calculate predictions per second
        if self.recent_predictions:
            total_recent = sum(count for _, count in self.recent_predictions)
            time_span = (current_time - self.recent_predictions[0][0]).total_seconds()
            self.metrics.predictions_per_second = total_recent / max(time_span, 1.0)
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics."""
        return {
            'total_predictions': self.metrics.total_predictions,
            'batch_predictions': self.metrics.batch_predictions,
            'avg_inference_time_ms': round(self.metrics.avg_inference_time_ms, 2),
            'avg_batch_inference_time_ms': round(self.metrics.avg_batch_inference_time_ms, 2),
            'error_count': self.metrics.error_count,
            'predictions_per_second': round(self.metrics.predictions_per_second, 2),
            'last_prediction_time': self.metrics.last_prediction_time.isoformat() if self.metrics.last_prediction_time else None,
            'current_model_version': self.current_version,
            'available_versions': list(self.version_manager.available_versions.keys())
        }
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get current model information."""
        if not self.current_metadata:
            return {'status': 'no_model_loaded'}
        
        return {
            'version': self.current_metadata.version,
            'model_type': self.current_metadata.model_type,
            'loaded_at': self.current_metadata.loaded_at.isoformat(),
            'file_path': self.current_metadata.file_path,
            'model_hash': self.current_metadata.model_hash,
            'feature_names': self.current_metadata.feature_names,
            'performance_metrics': self.current_metadata.performance_metrics,
            'training_date': self.current_metadata.training_date.isoformat() if self.current_metadata.training_date else None
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check on the ML predictor."""
        status = 'healthy'
        issues = []
        
        if not self.current_model:
            status = 'unhealthy'
            issues.append('No model loaded')
        
        if self.metrics.error_count > 0:
            error_rate = self.metrics.error_count / max(self.metrics.total_predictions, 1)
            if error_rate > 0.05:  # 5% error threshold
                status = 'degraded'
                issues.append(f'High error rate: {error_rate:.2%}')
        
        if self.metrics.avg_inference_time_ms > 1000:  # 1 second threshold
            status = 'degraded'
            issues.append(f'High inference time: {self.metrics.avg_inference_time_ms:.2f}ms')
        
        return {
            'status': status,
            'issues': issues,
            'metrics': self.get_performance_metrics(),
            'model_info': self.get_model_info()
        }
    
    def cleanup(self):
        """Cleanup resources."""
        if self.model_loader:
            self.model_loader.cleanup()

# Global instance for singleton pattern
_ml_predictor_instance: Optional[MLPredictor] = None
_predictor_lock = threading.Lock()

def get_ml_predictor() -> MLPredictor:
    """Get the global ML predictor instance."""
    global _ml_predictor_instance
    
    if _ml_predictor_instance is None:
        with _predictor_lock:
            if _ml_predictor_instance is None:
                _ml_predictor_instance = MLPredictor()
    
    return _ml_predictor_instance

# Test and example usage
if __name__ == '__main__':
    import asyncio
    from app.utils.logging_config import setup_logging, set_correlation_id
    
    setup_logging()
    set_correlation_id("ML_PREDICTOR_PRODUCTION_TEST")
    
    async def test_ml_predictor():
        """Test the production ML predictor."""
        try:
            # Create test model if it doesn't exist
            os.makedirs("ml_model", exist_ok=True)
            
            # Create dummy model for testing
            from sklearn.linear_model import LogisticRegression
            import numpy as np
            
            model_path = "ml_model/test_model_v1.pkl"
            if not os.path.exists(model_path):
                X_dummy = np.array([[100, 20, 1, 1], [2000, 60, 5, 3], [50, 80, 2, 1], [800, 40, 3, 2]])
                y_dummy = np.array([0, 1, 0, 1])
                dummy_model = LogisticRegression()
                dummy_model.fit(X_dummy, y_dummy)
                joblib.dump(dummy_model, model_path)
                
                # Create metadata file
                metadata = {
                    'version': 'test_model_v1',
                    'model_type': 'LogisticRegression',
                    'feature_names': ['total_charge_amount', 'patient_age', 'num_diagnoses', 'num_line_items'],
                    'performance_metrics': {'accuracy': 0.95, 'f1_score': 0.92},
                    'training_date': datetime.now().isoformat()
                }
                with open("ml_model/test_model_v1.json", 'w') as f:
                    json.dump(metadata, f, indent=2)
            
            # Test predictor
            predictor = MLPredictor()
            
            # Test single prediction
            sample_claim = {
                "claim_id": "TEST_001",
                "total_charge_amount": 150.75,
                "patient_age": 45,
                "diagnoses": [{"icd_code": "R51"}, {"icd_code": "M54.5"}],
                "line_items": [{"cpt_code": "99213", "units": 1}]
            }
            
            filters, probability = predictor.predict_filters(sample_claim)
            logger.info(f"Single prediction: {filters}, probability: {probability:.4f}")
            
            # Test batch prediction
            batch_claims = [sample_claim] * 15  # Create batch
            for i, claim in enumerate(batch_claims):
                claim["claim_id"] = f"BATCH_TEST_{i}"
            
            batch_results = predictor.predict_filters_batch(batch_claims)
            logger.info(f"Batch prediction completed for {len(batch_results)} claims")
            
            # Test performance monitoring
            metrics = predictor.get_performance_metrics()
            logger.info(f"Performance metrics: {metrics}")
            
            # Test health check
            health = predictor.health_check()
            logger.info(f"Health check: {health}")
            
            # Test model info
            model_info = predictor.get_model_info()
            logger.info(f"Model info: {model_info}")
            
            logger.info("All tests completed successfully")
            
        except Exception as e:
            logger.error(f"Test failed: {e}", exc_info=True)
        finally:
            predictor.cleanup()
    
    # Run async test
    asyncio.run(test_ml_predictor())