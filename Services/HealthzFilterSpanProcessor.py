import logging

from opentelemetry.sdk.trace import SpanProcessor

logger = logging.getLogger("HealthzFilterSpanProcessor")
class HealthzFilterSpanProcessor(SpanProcessor):

    def __init__(self, wrapped):
        self._wrapped = wrapped
    def on_start(self   , span, parent_context=None):
        logger.info(f"Span started: '{span.name}'")
        if "health" in span.name.lower():
            logger.info("Filtered out healthz span")
            return
        self._wrapped.on_start(span, parent_context)

    def on_end(self, span):
        logger.info(f"Span ended: '{span.name}'")
        if "health" in span.name.lower():
            logger.info("Filtered out healthz span")
            return

        self._wrapped.on_end(span)


    def shutdown(self):
        self._wrapped.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self._wrapped.force_flush(timeout_millis)
