import aiofiles.os
import os
import shutil
import csv
import io
from datetime import datetime

class FileService:
    @staticmethod
    async def ensure_clean_folder(folder_path: str):
        if not await aiofiles.ospath.exists(folder_path):
            await aiofiles.os.mkdir(folder_path)
        else:
            # Remove all files and subfolders in the folder
            for filename in os.listdir(folder_path):
                file_path = os.path.join(folder_path, filename)
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    await aiofiles.os.remove(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)

    @staticmethod
    async def join_csv_files_in_folder(folder_path: str, output_file: str):
        files = await aiofiles.os.listdir(folder_path)
        output_path = os.path.join(folder_path, output_file)
        first_file = True
        async with aiofiles.open(output_path, "w", encoding="utf-8") as outfile:
            for filename in files:
                file_path = os.path.join(folder_path, filename)
                if file_path == output_path or not await aiofiles.ospath.isfile(file_path):
                    continue
                async with aiofiles.open(file_path, "r", encoding="utf-8") as infile:
                    lines = await infile.readlines()
                    if not lines:
                        continue
                    if first_file:
                        await outfile.writelines(lines)
                        first_file = False
                    else:
                        # Skip header (first line)
                        await outfile.writelines(lines[1:])
                await aiofiles.os.remove(file_path)

    @staticmethod
    async def sort_csv_by_time(
            file_path: str,
            time_column: str = "Time",
            time_format: str = "%m/%d/%Y %H:%M:%S"
    ):
        # Read all rows
        async with aiofiles.open(file_path, mode="r", encoding="utf-8") as f:
            content = await f.read()
        rows = list(csv.reader(content.splitlines()))
        header, data = rows[0], rows[1:]
        time_idx = header.index(time_column)
        # Sort by parsed datetime
        data.sort(key=lambda row: datetime.strptime(row[time_idx], time_format))
        # Write sorted rows back
        output = io.StringIO()
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(data)
        async with aiofiles.open(file_path, mode="w", encoding="utf-8") as f:
            await f.write(output.getvalue())