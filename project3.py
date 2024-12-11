import argparse
import re
import subprocess
from pymongo import MongoClient
import pandas as pd
import os
from openpyxl import Workbook
from openpyxl.drawing.image import Image as OpenpyxlImage

# MongoDB connection setup
client = MongoClient('mongodb://localhost:27017/')
db = client['video_processing']
baselight_collection = db['baselight']
xytech_collection = db['xytech']

# Ensure necessary directories exist
os.makedirs("thumbnails", exist_ok=True)
os.makedirs("snippets", exist_ok=True)

# Function to calculate video length using FFprobe
def get_video_length(video_path):
    cmd = [
        'ffprobe', '-i', video_path, '-show_entries', 'format=duration', '-v', 'quiet', '-of', 'csv=p=0'
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    duration_seconds = float(result.stdout.strip())
    return duration_seconds

# Function to convert frames to timecode
def frame_to_timecode(frame, fps):
    hours = frame // (fps * 3600)
    minutes = (frame // (fps * 60)) % 60
    seconds = (frame // fps) % 60
    frames = frame % fps
    return f"{hours:02}:{minutes:02}:{seconds:02}:{frames:02}"

# Function to process the Xytech file
def process_xytech_file(xytech_filename):
    location_map = {}
    with open(xytech_filename, 'r') as xytech_file:
        producer = ''
        operator = ''
        job = ''
        for line in xytech_file:
            line = line.strip()
            if line.startswith('Producer:'):
                producer = line.split(':', 1)[1].strip()
            elif line.startswith('Operator:'):
                operator = line.split(':', 1)[1].strip()
            elif line.startswith('Job:'):
                job = line.split(':', 1)[1].strip()
            elif line:
                stripped_location = re.sub(r'^/hpsans\d+/production', '', line)
                location_map[stripped_location] = line

    xytech_collection.insert_one({
        'producer': producer,
        'operator': operator,
        'job': job,
        'locations': [{'stripped': k, 'full': v} for k, v in location_map.items()]
    })
    return location_map

# Function to process the Baselight file and populate the database
def process_baselight_file(baselight_filename, location_map):
    frames_locations = {}

    with open(baselight_filename, 'r') as baselight_file:
        for line in baselight_file:
            line = line.strip()
            if not line:
                continue

            parts = line.split()
            location_part = parts[0]
            frame_numbers = parts[1:]

            stripped_location = re.sub(r'^/baselightfilesystem1', '', location_part)

            if stripped_location in location_map:
                location_fixed = location_map[stripped_location]

                for frame in frame_numbers:
                    if frame.isdigit():
                        frames_locations[int(frame)] = location_fixed

    frames_sorted = sorted(frames_locations.items())
    i = 0
    while i < len(frames_sorted):
        start_frame = frames_sorted[i][0]
        location = frames_sorted[i][1]
        end_frame = start_frame

        while i + 1 < len(frames_sorted) and frames_sorted[i + 1][0] == end_frame + 1 and frames_sorted[i + 1][1] == location:
            end_frame = frames_sorted[i + 1][0]
            i += 1

        if start_frame == end_frame:
            baselight_collection.insert_one({
                'location': location,
                'frame': str(start_frame)
            })
        else:
            baselight_collection.insert_one({
                'location': location,
                'frame': f"{start_frame}-{end_frame}"
            })

        i += 1

# Function to generate a thumbnail for the middle frame of a range
def generate_thumbnail(video_filename, frame, output_path):
    cmd = [
        'ffmpeg', '-i', video_filename, '-vf', f"select=gte(n\,{frame})", '-vframes', '1',
        '-s', '96x74', output_path, '-y'
    ]
    subprocess.run(cmd, capture_output=True)

# Function to filter ranges, create thumbnails, write to XLS, and extract snippets
def filter_and_write_xls_and_snippets(video_filename, xls_filename, fps=24):
    video_length_seconds = get_video_length(video_filename)
    total_frames = int(video_length_seconds * fps)

    xytech_data = xytech_collection.find_one()
    producer = xytech_data.get('producer', '') if xytech_data else ''
    operator = xytech_data.get('operator', '') if xytech_data else ''
    job = xytech_data.get('job', '') if xytech_data else ''

    wb = Workbook()
    ws = wb.active
    ws.title = "Frames to Fix"

    ws_outside = wb.create_sheet("Out of Video Ranges")

    # Write metadata
    ws.append(["Producer", producer])
    ws.append(["Operator", operator])
    ws.append(["Job", job])
    ws.append([])
    ws.append(["Location", "Frames to Fix", "Timecode", "Thumbnail"])

    ws_outside.append(["Location", "Frames to Fix", "Timecode"])

    entries = list(baselight_collection.find({}))
    for entry in entries:
        frame_data = entry['frame']
        location = entry['location']

        if '-' in frame_data:  # Process ranges
            start, end = map(int, frame_data.split('-'))
            timecode = f"{frame_to_timecode(start, fps)} - {frame_to_timecode(end, fps)}"
            if start <= total_frames and end <= total_frames:
                # Valid ranges
                middle_frame = (start + end) // 2
                thumbnail_path = f"thumbnails/thumb_{start}_{end}.jpg"
                generate_thumbnail(video_filename, middle_frame, thumbnail_path)

                ws.append([location, frame_data, timecode])
                img = OpenpyxlImage(thumbnail_path)
                ws.add_image(img, f"D{ws.max_row}")

                snippet_path = f"snippets/{start}-{end}.mp4"
                cmd = [
                    'ffmpeg', '-i', video_filename,
                    '-ss', f"{start / fps}", '-to', f"{end / fps}",
                    '-c', 'copy', snippet_path
                ]
                subprocess.run(cmd, capture_output=True)
            else:
                # Out-of-range
                ws_outside.append([location, frame_data, timecode])
        else:
            # Individual frames
            frame = int(frame_data)
            timecode = frame_to_timecode(frame, fps)
            if frame > total_frames:
                ws_outside.append([location, frame_data, timecode])

    wb.save(xls_filename)
    print(f"XLS file created with thumbnails and out-of-range data: {xls_filename}")

# Main function with argparse
def main():
    parser = argparse.ArgumentParser(description="Process Baselight files, calculate timecodes, and generate XLS/snippets.")
    parser.add_argument('--xytech', required=True, help="Path to the Xytech file")
    parser.add_argument('--baselight', required=True, help="Path to the Baselight file")
    parser.add_argument('--process', help="Path to the video file for XLS/snippet creation")
    parser.add_argument('--outputXLS', help="Path to save the XLS file")
    args = parser.parse_args()

    # Process Xytech file
    location_map = process_xytech_file(args.xytech)

    # Process Baselight file
    process_baselight_file(args.baselight, location_map)

    if args.process and args.outputXLS:
        filter_and_write_xls_and_snippets(args.process, args.outputXLS)

if __name__ == '__main__':
    main()
