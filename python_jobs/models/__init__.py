"""
Data Models for Air Quality Data Platform.

This package provides Pydantic models and transformation functions for:
- openaq_models: OpenAQ API responses
- aqicn_models: AQICN API responses
"""

from .openaq_models import (
    OpenAQParameter,
    OpenAQParametersResponse,
    OpenAQLocation,
    OpenAQLocationsResponse,
    OpenAQSensor,
    OpenAQSensorsResponse,
    OpenAQMeasurement,
    OpenAQMeasurementsResponse,
    transform_parameter,
    transform_location,
    transform_sensor,
    transform_measurement
)

from .aqicn_models import (
    AQICNStation,
    AQICNMeasurement,
    AQICNTime,
    AQICNIAQI,
    transform_station,
    transform_measurement as transform_aqicn_measurement,
    get_all_pollutants,
    get_weather_parameters
)

__all__ = [
    # OpenAQ models
    "OpenAQParameter",
    "OpenAQParametersResponse",
    "OpenAQLocation",
    "OpenAQLocationsResponse",
    "OpenAQSensor",
    "OpenAQSensorsResponse",
    "OpenAQMeasurement",
    "OpenAQMeasurementsResponse",
    "transform_parameter",
    "transform_location",
    "transform_sensor",
    "transform_measurement",
    
    # AQICN models
    "AQICNStation",
    "AQICNMeasurement",
    "AQICNTime",
    "AQICNIAQI",
    "transform_station",
    "transform_aqicn_measurement",
    "get_all_pollutants",
    "get_weather_parameters"
]

