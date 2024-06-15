import argparse
import ffmpeg
import frameioclient
import os
import pandas as pd
import pymongo
import re
import subprocess
from frameioclient import FrameioClient
from openpyxl import load_workbook
from openpyxl.drawing.image import Image as Img

# Establish DB connection
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["Project3"]
collection1 = mydb["Collection1"] # Baselight
collection2 = mydb["Collection2"] # Xytech
collection3 = mydb["Collection3"]

# Method to insert files into MongoDB
def insert_into_mongodb(collection, data):
    collection.insert_many(data)

parser = argparse.ArgumentParser(description="Creating a detailed report for production")
parser.add_argument("--baselight", type=str, help="Baselight file to read")
parser.add_argument("--xytech", type=str, help="Xytech order file to read")
parser.add_argument("--insert_baselight", action="store_true")
parser.add_argument("--insert_xytech", action="store_true")
parser.add_argument("--process", type=str)
parser.add_argument("--output", action="store_false")

args = parser.parse_args()

# Inputting into the database
def populate_database():
    collection = collection1
    with open(args.baselight, "r") as f:
        lines = f.readlines()
        data = []
        for line in lines:
            record = {"line data": line.strip()}
            data.append(record)
        insert_into_mongodb(collection, data)
    print(f"{args.baselight} was successfully inserted into Collection 1")

    collection = collection2
    with open(args.xytech, "r") as f:
        lines = f.readlines()
        data = []
        for line in lines:
            record = {"line data": line.strip()}
            data.append(record)
        insert_into_mongodb(collection, data)
    print(f"{args.xytech} was successfully inserted into Collection 2")

# Getting duration in frames, needs to be 24 frames per second
def find_video_duration():
    probe = ffmpeg.probe(args.process)

    video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == "video"), None)
    if video_stream:
        frame_rate = 24
        duration_seconds = float(video_stream['duration'])
        duration_frames = int(duration_seconds * frame_rate)
        print(f"Duration in frames (24 fps): {duration_frames}")
        return duration_frames
    else:
        return None

# Using Project 1 code
def project1():
    locations = []
    frames = []
    cursor1 = collection1.find({})
    cursor2 = collection2.find({})
    for currentReadLine in cursor1:
        # print("Current: " + currentReadLine)
        # Each line will be an array split by spaces
        if 'line data' in currentReadLine:
            currentLine = currentReadLine['line data']
            parseLine = currentLine.split()
            # print(parseLine)
            # Separate the folder name from the rest of the line
            if parseLine:
                currentFolder = parseLine.pop(0)
                # Separate each location from the path
                parseFolder = currentFolder.split("/")
                # Leave only the path starting with Dune2
                parseFolder.pop(1)
                newFolder = "/".join(parseFolder)
                #print(f"test 1 {newFolder}")

                for techfile in cursor2:
                    if newFolder in techfile['line data']:
                        # print out a xytech path when a match is found with baselight
                        currentFolder = techfile['line data'].strip()
                        #print(currentFolder)
                        #locations.append(currentFolder)

            start = 0
            end = 0
            for number in parseLine:
                # print(number)
                # ignore if err or null is encountered
                if not number.isnumeric():
                    continue
                if start == 0:
                    start = number
                    continue
                if number == str(int(start) + 1):
                    end = number
                    continue
                elif number == str(int(end) + 1):
                    end = number
                    continue
                else:
                    if int(end) > 0:
                        #print(currentFolder, start + "-" + end)
                        locations.append(currentFolder)
                        frames.append(start + "-" + end)
                    else:
                        #print(currentFolder, start)
                        locations.append(currentFolder)
                        frames.append(start)
                    start = number
                    end = 0

            if int(end) > 0:
                locations.append(currentFolder)
                frames.append(start + "-" + end)
            else:
                locations.append(currentFolder)
                frames.append(start)
                start = number
                end = 0

    # Get rid of the extra line with zero
    locations.pop()
    frames.pop()
    #print(f"test 1 {locations}")
    #print(f"test 2 {frames}")

    create_main_file(locations, frames, collection3)

def create_main_file(locations, frames, collection):
    data = [{"location": loc, "frames": frame} for loc, frame in zip(locations, frames)]
    insert_into_mongodb(collection, data)
    print("Successfully created updated collection.")

# Drop entries that are not in the correct range or singular digits
def find_correct_ranges():
    cursor = collection3.find({})
    duration = find_video_duration()
    range_pattern = r'\b(\d+)-(\d+)\b'

    for doc in cursor:
        line = doc['frames']
        ranges = re.findall(range_pattern, line)

        if ranges:
            for range_match in ranges:
                if len(range_match) == 2:
                    start, end = map(int, range_match)
                    if start <= duration and end <= duration:
                        pass
                    else:
                        collection3.delete_one({'_id': doc['_id']})
                        break
                else:
                    pass
        else:
            singular_numbers = re.findall(r'\b\d+\b', line)
            if singular_numbers:
                collection3.delete_one({'_id': doc['_id']})
            else:
                pass

    print("Successfully cleaned number ranges.")

# Create a new field for timecodes
# Translate the frames and populate entries with this data
def extract_numbers(frames):
    numbers = []
    for item in frames.split(","):
        start, end = map(int, item.split("-"))
        numbers.extend([start, end])
    return numbers

def extract_numbers_str(frames):
    start_time, end_time = map(str.strip, frames.split("-"))
    return [start_time, end_time]

# Weekly Assignment 8
def convert(frames):
    total_seconds = frames // 24
    remaining_frames = frames % 24
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    # Get the timecode in the format HH:MM:SS:FF
    timecode = f"{hours:02d}:{minutes:02d}:{seconds:02d}:{remaining_frames:02d}"
    return timecode

def translate_frames():
    # Return data only for frames field
    cursor = collection3.find({}, {"frames": 1})
    for entry in cursor:
        frames_data = entry.get("frames", "")
        numbers = extract_numbers(frames_data)
        number1 = numbers[0]
        number1 = convert(number1)
        number2 = numbers[1]
        number2 = convert(number2)
        timecode_value = f"{number1} - {number2}"
        collection3.update_one({"_id": entry["_id"]}, {"$set": {"timecode": timecode_value}})
        #print(numbers)
    print("Successfully added timecodes.")

# Had to download mongoexport to export Excel file
# https://fastdl.mongodb.org/tools/db/mongodb-database-tools-windows-x86_64-100.9.4.zip
def export_xls():
    command = "mongoexport --db Project3 --collection Collection3 --out collection.json --jsonArray"
    result = subprocess.run(command, shell=True)
    # Check the return code
    if result.returncode == 0:
        print("Command executed successfully.")
    else:
        print("Error executing command:", result.returncode)
    df = pd.read_json('collection.json')
    df.to_excel('report.xlsx', index=False)
    print("Successfully created Excel file.")

# Getting images below

def convert_timecode(timecode):
    parts = timecode.split(':')
    hours = int(parts[0])
    minutes = int(parts[1])
    seconds = int(parts[2])
    frames = int(parts[3])
    milliseconds = int((frames / 24) * 1000)
    formatted_time = f"{hours:02d}:{minutes:02d}:{seconds:02d}.{milliseconds:03d}"

    return formatted_time

def get_images():
    image_directory = "C:/video-editing-automation"
    wb = load_workbook("report.xlsx")
    ws = wb.active

    # get timecode from Excel report file
    df = pd.read_excel("report.xlsx", sheet_name='Sheet1')
    existing_frames = df['frames']
    counter = 1
    # subtract the existing frames like 4 -2 and only then convert it to include milliseconds
    for value in existing_frames:
        df = pd.read_excel("report.xlsx")

        values = extract_numbers(value)
        value1 = values[0]
        value2 = values[1]
        middle_point = (value2 + value1) // 2
        converted_middle_point = convert(middle_point)
        correct_frame_for_image = convert_timecode(converted_middle_point)
        video = args.process
        output_filename = f"image{counter:02d}.png"
        command = f"ffmpeg -ss {correct_frame_for_image} -i {video} -vf scale=96:74 -vframes 1 {output_filename}"
        result = subprocess.run(command, shell=True)
        counter += 1
        # Error checking
        if result.returncode == 0:
            print("Command executed successfully.")
        else:
            print("Error executing command:", result.returncode)

    thumbnails = [file for file in os.listdir(image_directory) if file.endswith(".png")]

    for idx, image_filename in enumerate(thumbnails, start=1):
        cell = f"E{idx + 1}"
        img = Img(os.path.join(image_directory, image_filename))
        ws.add_image(img, cell)
    wb.save("report_with_images.xlsx")
    print("Created an Excel report with images!")

def get_videoshots():
    df = pd.read_excel("report.xlsx", sheet_name='Sheet1')
    existing_frames = df['timecode']
    counter = 1
    for value in existing_frames:
        df = pd.read_excel("report.xlsx")

        values = extract_numbers_str(value)
        value1 = values[0]
        value2 = values[1]
        # print(value1)
        # print(value2)
        start = convert_timecode(value1)
        end = convert_timecode(value2)
        video = args.process
        output_filename = f"video{counter:02d}.mp4"
        command = f"ffmpeg -i {video} -ss {start} -to {end} -y -c:a copy {output_filename}"
        result = subprocess.run(command, shell=True)
        counter += 1
        # Check the return code
        if result.returncode == 0:
            print("Command executed successfully.")
        else:
            print("Error executing command:", result.returncode)


def upload_videos():
    client = FrameioClient("YOURKEYHERE")

    video_directory = "C:/video-editing-automation"
    videos = [os.path.join(video_directory, file).replace("\\", "/") for file in os.listdir(video_directory) if
              file.startswith("video")]
    for video in videos:
        #print(video)
        asset = client.assets.upload(
            destination_id = "9aa16d8d-6c29-4c10-b6af-3360e60c7b95",
            filepath = video
        )



### Final Steps ###

# python project33.py --baselight Baselight_export.txt --xytech Xytech.txt

if args.baselight and args.xytech:
    populate_database()

# python project33.py --process TwitchVideo.mp4 --output

if args.process:
    find_video_duration()
    project1()
    find_correct_ranges()
    translate_frames()
    export_xls()
    get_images()
    get_videoshots()
    upload_videos()

myclient.close()