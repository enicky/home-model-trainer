import os
import sys
import tempfile

import aiofiles
from azure.core.settings import settings
from azure.core.tracing.ext.opentelemetry_span import OpenTelemetrySpan
from dapr.ext.fastapi import DaprApp
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi import Request, Depends

from model.startuploadmodel import StartUploadModel
from model.traceableevent import StartDownloadDataEvent
from util import extract_trace_context, dapr_event_dependency, publish_dapr_message, incremental_join_and_upload

settings.tracing_implementation = OpenTelemetrySpan

from Services.fileService import FileService
from constants import (PUBSUB_NAME, TOPIC_START_TRAIN_MODEL, TARGET_DOWNLOAD_FOLDER,
                       AI_START_DOWNLOAD_DATA, AI_FINISHED_DOWNLOAD_DATA,
                       AI_FINISHED_TRAIN_MODEL, AI_START_UPLOAD_MODEL)
from Services.azureService import AzureBlobService

import logging

from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from azure.monitor.opentelemetry.exporter import AzureMonitorTraceExporter

load_dotenv()

AI_CONNECTION_STRING = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING")
if not AI_CONNECTION_STRING:
    raise ValueError("APPLICATIONINSIGHTS_CONNECTION_STRING environment variable is not set.")

# Set up OpenTelemetry Tracing
resource = Resource.create({"service.name": "my-fastapi-service"})
from opentelemetry.sdk.trace.sampling import ALWAYS_ON

trace.set_tracer_provider(
    TracerProvider(
        resource=resource,
        sampler=ALWAYS_ON
    )
)

tracer = trace.get_tracer(__name__)

trace_exporter = AzureMonitorTraceExporter(connection_string=f"{AI_CONNECTION_STRING}")
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(trace_exporter))

# Manual test span for debugging
with tracer.start_as_current_span("manual-test-span"):
    logger = logging.getLogger(__name__)
    logger.info("Manual test span created. If you see this in logs but not in Application Insights, exporter/network is the issue.")


# Instrument FastAPI and requests
app = FastAPI()
FastAPIInstrumentor.instrument_app(app)
RequestsInstrumentor().instrument()
HTTPXClientInstrumentor().instrument()


# Set up logging (stdout only)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


dapr_app = DaprApp(app)

connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
if not connection_string:
    raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set.")
container_name = os.environ.get("AZURE_STORAGE_CONTAINER_NAME")
if not container_name:
    raise ValueError("AZURE_STORAGE_CONTAINER_NAME environment variable is not set.")
azure_service = AzureBlobService(connection_string, container_name)

@dapr_app.subscribe(PUBSUB_NAME, TOPIC_START_TRAIN_MODEL)
async def start_train_model(model_type: str = "a"):

    logger.info(f"Training {model_type}")
    delete_after_process = os.environ.get("DELETE_PROCESSED_BLOBS", "false").lower() == "true"
    result = await incremental_join_and_upload(azure_service, TARGET_DOWNLOAD_FOLDER, delete_after_process=delete_after_process)
    logger.info(result['status'])
    logger.info("Training completed")
    await publish_dapr_message(PUBSUB_NAME, AI_FINISHED_TRAIN_MODEL, {"model_path": ""}) #Must still fill in model path ...
    logger.info("Finished training model and sent message back")


@dapr_app.subscribe(PUBSUB_NAME, AI_START_UPLOAD_MODEL)
async def start_upload_model(startUploadModel: StartUploadModel):
    logger.info(f"Received StartUploadModel: {startUploadModel}")
    logger.info(f"traceparent header: {startUploadModel.traceparent if startUploadModel.traceparent else 'None'}")
    parent_context = extract_trace_context(getattr(startUploadModel, 'traceparent', None), getattr(startUploadModel, 'tracestate', None))
    with tracer.start_as_current_span("start_upload_model", context=parent_context) as span:
        logger.info("Processing message, trace_id:", span.get_span_context().trace_id)
        logger.info(f"traceparent: {getattr(startUploadModel, 'traceparent', None)}")
        logger.info("Starting model upload")
        logger.info(f"Current time: {startUploadModel.trigger_moment}")
        logger.info("Finished uploading model and sent message back")

@dapr_app.subscribe(PUBSUB_NAME, AI_START_DOWNLOAD_DATA)
async def start_download_data(
    event: StartDownloadDataEvent = Depends(dapr_event_dependency(StartDownloadDataEvent)),
    request: Request = None
):
    raw_body = await request.body() if request else b''
    logger.info(f"Raw request body: {raw_body.decode() if raw_body else ''}")
    logger.info(f"Received StartDownloadDataEvent: {event.model_dump_json()}")
    parent_context = extract_trace_context(getattr(event, 'TraceParent', None), getattr(event, 'TraceState', None))
    with tracer.start_as_current_span("start_download_data", context=parent_context) as span:
        logger.info("Processing message, trace_id:", span.get_span_context().trace_id)
        logger.info(f"traceparent: {getattr(event, 'TraceParent', None) if getattr(event, 'TraceParent', None) else 'No TraceParent provided'}")
        logger.info("Starting data download")
        logger.info("Nothing to do. This is a placeholder for future download logic.")
        await publish_dapr_message(PUBSUB_NAME, AI_FINISHED_DOWNLOAD_DATA, {"finished": True})
        logger.info("Data download completed and sent message back")


@app.get("/")
async def root():
    return {"message": "Hello World"}
