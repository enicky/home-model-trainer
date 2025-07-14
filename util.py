from dapr.clients import DaprClient
from fastapi import Request
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
import json
import os
import tempfile
import aiofiles

from Services.fileService import FileService


def extract_trace_context(traceparent: str = None, tracestate: str = None):
    carrier = {}
    if traceparent:
        carrier["traceparent"] = traceparent
    if tracestate:
        carrier["tracestate"] = tracestate
    parent_context = TraceContextTextMapPropagator().extract(carrier=carrier)
    return parent_context


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

async def publish_dapr_message(pubsub_name, topic, data):
    with DaprClient() as d:
        resp = d.publish_event(
            pubsub_name=pubsub_name,
            topic_name=topic,
            data=json.dumps(data),
            data_content_type='application/json'
        )

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
