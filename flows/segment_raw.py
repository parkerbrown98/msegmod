from typing import List, Tuple
from prefect import task, flow
from scenedetect import open_video, SceneManager, ContentDetector

import os


@task(log_prints=True)
def detect_scene_segments(video_path: str, threshold: float = 27.0):
    video = open_video(video_path)
    scene_manager = SceneManager()
    scene_manager.add_detector(ContentDetector(threshold=threshold))

    scene_manager.detect_scenes(video)
    scenes = scene_manager.get_scene_list()

    return [(scene[0].get_seconds(), scene[1].get_seconds()) for scene in scenes]


@task
def save_segments(segments: List[Tuple[int, int]], output_path: str):
    absolute_output_path = os.path.abspath(output_path)
    os.makedirs(os.path.dirname(absolute_output_path), exist_ok=True)
    with open(output_path, "w") as f:
        for segment in segments:
            f.write(f"{segment[0]},{segment[1]}\n")


@flow
def segment_raw(video_path: str, output_path: str, threshold: float = 30.0):
    segments = detect_scene_segments(video_path, threshold)
    save_segments(segments, output_path)


if __name__ == "__main__":
    segment_raw(video_path="video.mp4", output_path="segments.txt")
