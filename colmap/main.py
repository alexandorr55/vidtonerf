from flask import Flask
from flask import send_from_directory
from pathlib import Path
from video_to_images import split_video_into_frames
from colmap_runner import run_colmap
from matrix import get_json_matrices
from image_position_extractor import extract_position_data
import requests
import pika
import json
import time
from multiprocessing import Process
import os


app = Flask(__name__)
base_url = "http://localhost:5100/"

@app.route('/data/imgs/<path:path>')
def send_video(path):
    return send_from_directory('data/outputs/',path)


def start_flask():
    global app
    app.run(host="localhost", port=5100,debug=True)

def to_url(local_file_path: str):
    return base_url+"/data/imgs/"+local_file_path


def colmap_worker():
    credentials = pika.PlainCredentials('admin', 'password123')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials,heartbeat=600)

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='sfm-in')
    channel.queue_declare(queue='sfm-out')
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
        video = requests.get(job_data['file_path'])
        video_file_path = f"{input_data_dir}{id}.mp4"
        open(video_file_path,"wb").write(video.content)
        
        #run colmap and save data to custom directory
        #(1) vid_to_images.py
        # TODO: fix this path and make fps dynamic
        ffmpeg_path= "/usr/bin/ffmpeg"
        ffmpeg_out_id = f"ffmpeg-{id}"
        status = split_video_into_frames(ffmpeg_out_id, output_data_dir, ffmpeg_path, video_file_path, fps=24)
        if status == 0:
            print("ffmpeg ran successfully.")
        elif status == 1:
            print("ERROR: There was an unknown error running ffmpeg")
        elif status == 2:
            print(f"ERROR: ffmpeg - file {output_data_dir}/{ffmpeg_out_id} already exists.")
        elif status == 3:
            print(f"ERROR: ffmpeg - file {output_data_dir} could not be found.")
        # imgs are now in output_data_dir/id

        #(2) colmap_runner.py
        colmap_path = "/usr/local/bin/colmap"
        images_path = os.path.join(output_data_dir,ffmpeg_out_id)
        colmap_out_id = f"colmap-{id}"
        colmap_output_path = os.path.join(output_data_dir,colmap_out_id)
        status = run_colmap(colmap_out_id, output_data_dir, colmap_path, images_path)
        if status == 0:
            print("COLMAP ran successfully.")
        elif status == 1:
            print("ERROR: There was an unknown error running COLMAP")
        elif status == 2:
            print(f"ERROR: COLMAP - file {output_data_dir}/{colmap_out_id} already exists.")
        elif status == 3:
            print(f"ERROR: COLMAP - file {output_data_dir} could not be found.")
        
        #(3) matrix.py
        initial_motion_path = os.path.join(colmap_output_path,"images.txt")
        camera_stats_path = os.path.join(colmap_output_path,"cameras.txt")
        parsed_motion_path = os.path.join(colmap_output_path,"parsed_data.csv")

        extract_position_data(initial_motion_path, parsed_motion_path)
        motion_data = get_json_matrices(camera_stats_path, parsed_motion_path)
        motion_data["id"] = id

        # Save copy of motion data
        with open(os.path.join(colmap_output_path,"transforms_data.json"), 'w') as outfile:
            outfile.write(json.dumps(motion_data, indent=4))


        # create links to local data to serve
        for i,frame in enumerate(motion_data["frames"]):
            file_name = frame["file_path"]
            file_path = os.path.join(ffmpeg_out_id,file_name)
            file_url = to_url(file_path)
            motion_data["frames"][i]["file_path"] = file_url

        json_motion_data = json.dumps(motion_data)
        channel.basic_publish(exchange='', routing_key='sfm-out', body=json_motion_data)

        # confirm to rabbitmq job is done
        ch.basic_ack(delivery_tag=method.delivery_tag)




    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='sfm-in', on_message_callback=process_colmap_job)
    channel.start_consuming()
    print("should not get here")



if __name__ == "__main__":
    flaskProcess = Process(target=start_flask, args= ())
    nerfProcess = Process(target=colmap_worker, args= ())
    flaskProcess.start()
    nerfProcess.start()
    flaskProcess.join()
    nerfProcess.join()