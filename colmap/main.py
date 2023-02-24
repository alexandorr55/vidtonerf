from flask import Flask
from flask import send_from_directory
from pathlib import Path
from video_to_images import split_video_into_frames
from colmap_runner import run_colmap
from matrix import get_json_matrices
from image_position_extractor import extract_position_data
from opt import config_parser
import requests
import pika
import json
import time
from multiprocessing import Process
import os
import argparse
import sys


app = Flask(__name__)
# base_url = "http://host.docker.internal:5000/"
base_url = "http://sfm-worker:5100/"


@app.route("/data/outputs/<path:path>")
def send_video(path):
    return send_from_directory("data/outputs/", path)


def start_flask():
    global app
    app.run(host="0.0.0.0", port=5100, debug=True)


def to_url(local_file_path: str):
    return base_url + local_file_path

def run_full_sfm_pipeline(id, output_data_dir): 
    #run colmap and save data to custom directory
    # Create output directory under data/output_data_dir/id
    # TODO: use library to fix filepath joining
    if not output_data_dir.endswith(("\\", "/")) and not id.startswith(("\\", "/")):
        output_data_dir = output_data_dir + "/"
    output_path = output_data_dir + id
    Path(f"{output_path}").mkdir(parents=True, exist_ok=True)


    #(1) get stuff from web-server
    ## Before, split_video_into_frames would run here
    ## The frames would then be saved to output_data_dir/id
    ## This has to take the images from that directory
    #TODO: Make this get images from output_data_dir/id
    imgs_folder = os.path.join(output_path, "imgs")

    #(2) colmap_runner.py
    colmap_path = "/usr/local/bin/colmap"
    status = run_colmap(colmap_path, imgs_folder, output_path)
    if status == 0:
        print("COLMAP ran successfully.")
    elif status == 1:
        print("ERROR: There was an unknown error running COLMAP")

    # (3) matrix.py
    initial_motion_path = os.path.join(output_path, "images.txt")
    camera_stats_path = os.path.join(output_path, "cameras.txt")
    parsed_motion_path = os.path.join(output_path, "parsed_data.csv")

    extract_position_data(initial_motion_path, parsed_motion_path)
    motion_data = get_json_matrices(camera_stats_path, parsed_motion_path)
    motion_data["id"] = id

    # Save copy of motion data
    with open(os.path.join(output_path, "transforms_data.json"), "w") as outfile:
        outfile.write(json.dumps(motion_data, indent=4))

    return motion_data, imgs_folder


def colmap_worker(use_rabbitmq=False):
    input_data_dir = "data/inputs/"
    output_data_dir = "data/outputs/"
    Path(f"{input_data_dir}").mkdir(parents=True, exist_ok=True)
    Path(f"{output_data_dir}").mkdir(parents=True, exist_ok=True)

    def process_colmap_job(ch, method, properties, body):
        print("Starting New Job")
        print(body.decode())
        job_data = json.loads(body.decode())
        id = job_data["id"]
        print(f"Running New Job With ID: {id}")
        
        #TODO: Handle exceptions and enable steaming to make safer 
        images_or_something = requests.get(job_data['file_path'], timeout=10)
        print("Web server pinged")
        
        #TODO: Get json images from web server
        
        # RUNS COLMAP AND CONVERSION CODE
        motion_data, imgs_folder = run_full_sfm_pipeline(id, output_data_dir)

        # create links to local data to serve
        for i,frame in enumerate(motion_data["frames"]):
            file_name = frame["file_path"]
            file_path = os.path.join(imgs_folder,file_name)
            file_url = to_url(file_path)
            motion_data["frames"][i]["file_path"] = file_url

        json_motion_data = json.dumps(motion_data)
        #TODO: figure out what this does
        channel.basic_publish(exchange='', routing_key='sfm-out', body=json_motion_data)


if __name__ == "__main__":
    input_data_dir = "data/inputs/"
    output_data_dir = "data/outputs/"
    Path(f"{input_data_dir}").mkdir(parents=True, exist_ok=True)
    Path(f"{output_data_dir}").mkdir(parents=True, exist_ok=True)

    # Load args from config file
    args = config_parser()

    # Local run behavior
    if args.local_run == True:
        nerfProcess = Process(target=colmap_worker, args=())
        nerfProcess.start()
        motion_data, imgs_folder = run_full_sfm_pipeline(
            "Local_Test", args.input_data_path, input_data_dir, output_data_dir
        )
        print(motion_data)
        json_motion_data = json.dumps(motion_data)

    # Standard webserver run behavior
    else:
        nerfProcess = Process(target=colmap_worker, args=())
        flaskProcess = Process(target=start_flask, args=())
        flaskProcess.start()
        nerfProcess.start()
        flaskProcess.join()
        nerfProcess.join()
