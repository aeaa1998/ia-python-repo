import boto3
import json
import cv2

with open('credentials.json') as f:
    credentials = json.load(f)

clientS3 = boto3.client('s3',
                        aws_access_key_id=credentials["aws_access_key_id"],
                        aws_secret_access_key=credentials["aws_secret_access_key"],
                        region_name='us-east-2')

client = boto3.client('rekognition',
                      aws_access_key_id=credentials["aws_access_key_id"],
                      aws_secret_access_key=credentials["aws_secret_access_key"],
                      region_name='us-east-2')


personsTrackResponse = {
    "Mizutani": [],
    "Boll": [],
}
frameResponses = {}
dirtyResponses = {}
MIZUTANI_INDEX = 1
BOLL_INDEX = 0


def get_height_in_cm(movement):
    return (float(movement) * 181.0) / 0.40289023518562317


def get_width_in_cm(movement):
    return (float(movement) * 152.4) / 0.24


def get_velocity(distance, time=1/24):
    return distance / time


def build_json_person(index, name, person_movement_index):
    person = label['Instances'][index]
    person_json = {
        "BoundingBox": person["BoundingBox"],
        "Confidence": person["Confidence"],
        "movement": {
            "y": 0.0,
            "x": 0.0,
            "y_speed": 0.0,
            "x_speed": 0.0,
            "y_speed_avg": 0.0,
            "x_speed_avg": 0.0
        },
        "past_speeds": {
            "y": [],
            "x": []
        }
    }
    if len(personsTrackResponse[name]) != 0:
        past_person = personsTrackResponse[name][person_movement_index]
        past_left = float(past_person["BoundingBox"]["Left"])
        new_left = float(person["BoundingBox"]["Left"])
        past_top = float(past_person["BoundingBox"]["Top"])
        new_top = float(person["BoundingBox"]["Top"])
        person_json["movement"]["x"] = get_width_in_cm(new_left - past_left)
        person_json["movement"]["y"] = get_width_in_cm(new_top - past_top)
        person_json["movement"]["y_speed"] = get_velocity(abs(person_json["movement"]["y"]))
        person_json["movement"]["x_speed"] = get_velocity(abs(person_json["movement"]["x"]))
        if len(personsTrackResponse[name]) > 1:
            person_json["past_speeds"]["x"] = past_person["past_speeds"]["x"].copy()
            person_json["past_speeds"]["x"].append(person_json["movement"]["x_speed"])
            person_json["past_speeds"]["y"] = past_person["past_speeds"]["y"].copy()
            person_json["past_speeds"]["y"].append(person_json["movement"]["y_speed"])
            person_json["movement"]["y_speed_avg"] = sum(person_json["past_speeds"]["y"])/len(person_json["past_speeds"]["y"])
            person_json["movement"]["x_speed_avg"] = sum(person_json["past_speeds"]["x"]) / len(person_json["past_speeds"]["x"])
        else:
            person_json["past_speeds"]["x"].append(person_json["movement"]["x_speed"])
            person_json["past_speeds"]["y"].append(person_json["movement"]["y_speed"])
            person_json["movement"]["y_speed_avg"] = person_json["movement"]["y_speed"]
            person_json["movement"]["x_speed_avg"] = person_json["movement"]["x_speed"]

    return person_json


person_movement_index = 0
for i in range(5, 512):
    # We are going to capture each half a second some information
    if i % 1 == 0:
        with open("frames/pp/frame" + str(i) + ".jpg", 'rb') as source_image:
            response = client.detect_labels(
                Image={
                    'Bytes': source_image.read()
                },
                MaxLabels=8,
                MinConfidence=60
            )
            frameResponses[i] = {}
            dirtyResponses[i] = {}
            for label in response['Labels']:
                dirtyResponses[i][label['Name']] = {
                    'accuracy': label['Confidence'],
                    'name': label['Name'],
                    'instances': label['Instances']
                }
                # We are going to pass only if there are any instances
                if len(label['Instances']) != 0:
                    # If it is a person it should be registered
                    if label['Name'] == "Person" and len(label['Instances']) >= 2:
                        t_1 = label['Instances'][0]['BoundingBox']['Top']
                        t_2 = label['Instances'][1]['BoundingBox']['Top']
                        if t_1 > t_2:
                            BOLL_INDEX = 0
                            MIZUTANI_INDEX = 1
                        else:
                            BOLL_INDEX = 1
                            MIZUTANI_INDEX = 0

                        mizutani_json = build_json_person(MIZUTANI_INDEX, "Mizutani", person_movement_index)
                        mizutani_json["Frame"] = i
                        mizutani_json["Second"] = i/24
                        personsTrackResponse["Mizutani"].append(mizutani_json)
                        boll_json = build_json_person(BOLL_INDEX, "Boll", person_movement_index)
                        boll_json["Frame"] = i
                        boll_json["Second"] = i / 24
                        personsTrackResponse["Boll"].append(boll_json)
                        if len(personsTrackResponse["Mizutani"]) > 1:
                            person_movement_index += 1

                    frameResponses[i][label['Name']] = {
                        'accuracy': label['Confidence'],
                        'name': label['Name'],
                        'instances': label['Instances']
                    }
with open('results/pp/personsTrack.json', 'w') as outfile:
    json.dump(personsTrackResponse, outfile, indent=4)
with open('results/pp/results.json', 'w') as outfile:
    json.dump(frameResponses, outfile, indent=4)
with open('results/pp/dirty-results.json', 'w') as outfile:
    json.dump(frameResponses, outfile, indent=4)
# response = client.detect_labels(
#     Image={
#         'S3Object': {
#             'Bucket': 'tennisproyecto',
#             'Name': 'dumb.jpeg',
#         }
#     },
#     MaxLabels=12,
#     MinConfidence= 1
# )
# response
