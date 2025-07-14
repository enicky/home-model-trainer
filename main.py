import os
import sys
import tempfile
from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional
from fastapi import Request, Depends

import aiofiles
import json
from dotenv import load_dotenv
from fastapi import FastAPI
from dapr.ext.fastapi import DaprApp
from dapr.clients import DaprClient
from azure.core.settings import settings
from azure.core.tracing.ext.opentelemetry_span import OpenTelemetrySpan

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
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

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

class ModelType:
    def __init__(self, model_type: str):
        self.model_type = model_type

async def incremental_join_and_upload(azure_service, target_folder, joined_blob_name='joined/all_data.csv', manifest_blob_name='joined/processed_blobs.txt', delete_after_process=False):
    # Step 1: Download manifest
    processed_blobs = set()
    manifest_content = await azure_service.download_text_blob(manifest_blob_name)
    if manifest_content:
        processed_blobs = set(manifest_content.strip().split('\n'))

    # Step 2: List all blobs in source
    all_blobs = await azure_service.list_blob_names()
    new_blobs = [b for b in all_blobs if b not in processed_blobs and not b.startswith('joined/')]
    if not new_blobs:
        return {'status': 'No new blobs to process.'}

    # Step 3: Download new blobs to temp folder
    with tempfile.TemporaryDirectory() as tempdir:
        await azure_service.download_blobs_by_names(tempdir, new_blobs)
        # Step 4: Download current joined CSV if exists
        joined_csv_path = os.path.join(tempdir, 'all_data.csv')
        joined_csv_content = await azure_service.download_text_blob(joined_blob_name)
        if joined_csv_content:
            async with aiofiles.open(joined_csv_path, 'w', encoding='utf-8') as f:
                await f.write(joined_csv_content)
        # Step 5: Join new CSVs into joined_csv_path
        await FileService.join_csv_files_in_folder(tempdir, 'all_data.csv')
        # Step 5b: Sort the joined CSV by time
        await FileService.sort_csv_by_time(joined_csv_path)
        # Step 6: Upload updated joined CSV
        async with aiofiles.open(joined_csv_path, 'r', encoding='utf-8') as f:
            new_joined_content = await f.read()
        await azure_service.upload_text_blob(joined_blob_name, new_joined_content)
    # Step 7: Update and upload manifest
    processed_blobs.update(new_blobs)
    await azure_service.upload_text_blob(manifest_blob_name, '\n'.join(processed_blobs))

    # Step 8: Optionally delete processed blobs
    if delete_after_process and new_blobs:
        await azure_service.delete_blobs_by_names(new_blobs)
        return {'status': f'Processed and deleted {len(new_blobs)} new blobs and updated joined CSV.'}
    return {'status': f'Processed {len(new_blobs)} new blobs and updated joined CSV.'}

async def publish_dapr_message(pubsub_name, topic, data):
    with DaprClient() as d:
        resp = d.publish_event(
            pubsub_name=pubsub_name,
            topic_name=topic,
            data=json.dumps(data),
            data_content_type='application/json'
        )

@dapr_app.subscribe(PUBSUB_NAME, TOPIC_START_TRAIN_MODEL)
async def start_train_model(model_type: str = "a"):
    logger.info(f"Training {model_type}")
    delete_after_process = os.environ.get("DELETE_PROCESSED_BLOBS", "false").lower() == "true"
    result = await incremental_join_and_upload(azure_service, TARGET_DOWNLOAD_FOLDER, delete_after_process=delete_after_process)
    logger.info(result['status'])
    logger.info("Training completed")
    await publish_dapr_message(PUBSUB_NAME, AI_FINISHED_TRAIN_MODEL, {"model_path": ""}) #Must still fill in model path ...
    logger.info("Finished training model and sent message back")



class StartUploadModel(BaseModel):
    
    def __init__(self):
        super().__init__()
        self.model_path = ""
        self.trigger_moment = datetime.now()
        
    def __init__(self, model_path: str = "", trigger_moment: datetime = None):
        super().__init__()
        self.model_path = model_path
        self.trigger_moment = trigger_moment or datetime.now()


@dapr_app.subscribe(PUBSUB_NAME, AI_START_UPLOAD_MODEL)
async def start_upload_model(startUploadModel: StartUploadModel):
    # prepare a carrier for the propagator
    carrier = {}
    logger.info(f"Received StartUploadModel: {startUploadModel}")
    logger.info(f"traceparent header: {startUploadModel.traceparent if startUploadModel.traceparent else 'None'}")
    if startUploadModel.traceparent:
        carrier["traceparent"] = startUploadModel.traceparent
        
    # Use the propagator to extract the traceparent header
    parent_context = TraceContextTextMapPropagator().extract(carrier=carrier)
    
    with tracer.start_as_current_span("start_upload_model", context=parent_context):
        traceparent = startUploadModel.traceparent if startUploadModel.traceparent else "No traceparent provided"
        logger.info(f"traceparent: {startUploadModel.traceparent}")
        logger.info("Starting model upload")
        logger.info(f"Current time: {startUploadModel.trigger_moment}")
        logger.info("Finished uploading model and sent message back")

class TraceableEvent(BaseModel):
    traceparent: Optional[str] = Field("", alias="traceparent")

class StartDownloadDataEvent(TraceableEvent):
    pass

def dapr_event_dependency(model_class):
    async def dependency(request: Request):
        raw_body = await request.body()
        print(f"[dapr_event_dependency] Raw request body: {raw_body}")
        try:
            cloud_event = json.loads(raw_body)
            print(f"[dapr_event_dependency] Parsed CloudEvent: {cloud_event}")
        except Exception as e:
            print(f"[dapr_event_dependency] Failed to parse CloudEvent: {e}")
            raise
        data = cloud_event.get("data")
        print(f"[dapr_event_dependency] CloudEvent 'data' field: {data} (type: {type(data)})")
        if isinstance(data, str):
            try:
                data = json.loads(data)
                print(f"[dapr_event_dependency] Parsed 'data' as JSON: {data}")
            except Exception as e:
                print(f"[dapr_event_dependency] Failed to parse 'data' as JSON: {e}")
                raise
        else:
            print(f"[dapr_event_dependency] 'data' is not a string, using as is.")
        try:
            event_obj = model_class(**data)
            print(f"[dapr_event_dependency] Instantiated event object: {event_obj}")
        except Exception as e:
            print(f"[dapr_event_dependency] Failed to instantiate event object: {e}")
            raise
        return event_obj
    return dependency

@dapr_app.subscribe(PUBSUB_NAME, AI_START_DOWNLOAD_DATA)
async def start_download_data(
    event: StartDownloadDataEvent = Depends(dapr_event_dependency(StartDownloadDataEvent)),
    request: Request = None
):
    raw_body = await request.body() if request else b''
    logger.info(f"Raw request body: {raw_body.decode() if raw_body else ''}")
    logger.info(f"Received StartDownloadDataEvent: {event.model_dump_json()}")
    carrier = {}
    if event.traceparent:
        carrier["traceparent"] = event.traceparent
    parent_context = TraceContextTextMapPropagator().extract(carrier=carrier)
    with tracer.start_as_current_span("start_download_data", context=parent_context):
        logger.info(f"traceparent: {event.traceparent if event.traceparent else 'No traceparent provided'}")
        logger.info("Starting data download")
        logger.info("Nothing to do. This is a placeholder for future download logic.")
        await publish_dapr_message(PUBSUB_NAME, AI_FINISHED_DOWNLOAD_DATA, {"finished": True})
        logger.info("Data download completed and sent message back")


@app.get("/")
async def root():
    return {"message": "Hello World"}
