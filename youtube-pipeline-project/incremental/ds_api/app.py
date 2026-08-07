import csv
from pathlib import Path

from fastapi import FastAPI

DATA_PATH = Path(__file__).parent / "data" / "ds_prime_videos.csv"

app = FastAPI(title="DS' historical slice API")

with open(DATA_PATH, encoding="utf-8") as f:
    VIDEOS = list(csv.DictReader(f))


@app.get("/health")
def health():
    return {"status": "ok", "video_count": len(VIDEOS)}


@app.get("/ds-videos")
def get_ds_videos():
    return VIDEOS
