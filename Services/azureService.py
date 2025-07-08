import os
from azure.storage.blob.aio import BlobServiceClient

class AzureBlobService:
    def __init__(self, connection_string: str, container_name: str):
        self.connection_string = connection_string
        self.container_name = container_name
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.blob_service_client.get_container_client(container_name)

    def safe_join(self, bd: str, filename: str) -> str:
        filename = os.path.basename(filename)
        full_path = os.path.join(bd, filename)
        if os.path.commonpath([os.path.abspath(full_path), os.path.abspath(bd)]) != os.path.abspath(bd):
            raise ValueError("Invalid file path")
        return full_path

    async def download_blobs(self, download_folder: str):
        import os
        os.makedirs(download_folder, exist_ok=True)
        file_names = []
        async for blob in self.container_client.list_blobs():
            blob_client = self.container_client.get_blob_client(blob)
            file_path = self.safe_join(download_folder, blob.name)
            stream = await blob_client.download_blob()
            data = await stream.readall()
            with open(file_path, "wb") as f:
                f.write(data)
            file_names.append(blob.name)
        return {"folder": download_folder, "files": file_names}

    async def download_text_blob(self, blob_name: str) -> str:
        blob_client = self.container_client.get_blob_client(blob_name)
        try:
            stream = await blob_client.download_blob()
            return (await stream.readall()).decode('utf-8')
        except Exception:
            return ''

    async def upload_text_blob(self, blob_name: str, text: str):
        blob_client = self.container_client.get_blob_client(blob_name)
        await blob_client.upload_blob(text, overwrite=True)

    async def list_blob_names(self, prefix: str = None):
        blob_names = []
        async for blob in self.container_client.list_blobs(name_starts_with=prefix):
            blob_names.append(blob.name)
        return blob_names

    async def download_blobs_by_names(self, download_folder: str, blob_names: list):
        import os
        os.makedirs(download_folder, exist_ok=True)
        file_names = []
        for blob_name in blob_names:
            blob_client = self.container_client.get_blob_client(blob_name)
            file_path = self.safe_join(download_folder, blob_name)
            stream = await blob_client.download_blob()
            data = await stream.readall()
            with open(file_path, "wb") as f:
                f.write(data)
            file_names.append(blob_name)
        return {"folder": download_folder, "files": file_names}

    async def delete_blobs_by_names(self, blob_names: list):
        for blob_name in blob_names:
            blob_client = self.container_client.get_blob_client(blob_name)
            await blob_client.delete_blob()