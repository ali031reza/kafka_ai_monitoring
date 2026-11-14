"""DeepStream pipeline integration for Kafka AI Monitoring.

The package is intentionally isolated from the existing Kafka frame workers.
DeepStream runtime imports are kept out of package initialization so config
and planning utilities can be tested without an NVIDIA runtime.
"""
