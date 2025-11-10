import requests
import io
from zipfile import ZipFile
from pathlib import Path


KAGGLE_API_URL = "https://www.kaggle.com/api/v1/datasets/download/muthuj7/weather-dataset"
TEMP_PATH = Path() / "tmp"


def extract():
    response = requests.get(KAGGLE_API_URL, stream=True)
    bytes_io = io.BytesIO(response.content)

    with ZipFile(bytes_io, "r") as zip_file:
        filename = zip_file.filelist[0].filename
        zip_file.extractall(TEMP_PATH)

    return filename

if __name__ == "__main__":
    extract()