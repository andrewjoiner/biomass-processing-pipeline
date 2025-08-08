#!/usr/bin/env python3
"""
Logging Utilities v1 - Structured Logging for Biomass Processing
Clean logging configuration and utilities for production monitoring
"""

import logging
import os
import sys
from datetime import datetime
from typing import Dict, Optional

def setup_logging(log_level: str = 'INFO', log_file: Optional[str] = None,
                 include_timestamp: bool = True, include_module: bool = True) -> logging.Logger:
    """
    Set up structured logging for biomass processing
    
    Args:
        log_level: Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR')
        log_file: Optional log file path
        include_timestamp: Include timestamps in log messages
        include_module: Include module names in log messages
        
    Returns:
        Configured logger instance
    """
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create formatter
    format_parts = []
    if include_timestamp:
        format_parts.append('%(asctime)s')
    if include_module:
        format_parts.append('%(name)s')
    format_parts.extend(['%(levelname)s', '%(message)s'])
    
    formatter = logging.Formatter(' - '.join(format_parts))
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        # Create log directory if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def get_processing_logger(component_name: str, processing_id: str) -> logging.Logger:
    """
    Get a logger for a specific processing component
    
    Args:
        component_name: Name of the processing component
        processing_id: Unique processing identifier
        
    Returns:
        Configured logger with component context
    """
    logger_name = f"biomass.{component_name}.{processing_id}"
    return logging.getLogger(logger_name)

class ProcessingMetrics:
    """Simple metrics collection for processing monitoring"""
    
    def __init__(self, component_name: str):
        self.component_name = component_name
        self.metrics = {}
        self.start_time = datetime.now()
        
    def increment(self, metric_name: str, value: int = 1):
        """Increment a counter metric"""
        if metric_name not in self.metrics:
            self.metrics[metric_name] = 0
        self.metrics[metric_name] += value
    
    def set_gauge(self, metric_name: str, value: float):
        """Set a gauge metric value"""
        self.metrics[metric_name] = value
    
    def get_metrics(self) -> Dict:
        """Get all metrics with processing duration"""
        duration = (datetime.now() - self.start_time).total_seconds()
        return {
            'component': self.component_name,
            'processing_duration_seconds': duration,
            'metrics': self.metrics.copy(),
            'timestamp': datetime.now().isoformat()
        }
    
    def log_metrics(self, logger: logging.Logger):
        """Log current metrics"""
        metrics_data = self.get_metrics()
        logger.info(f"Processing metrics: {metrics_data}")