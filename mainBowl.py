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

bowlingBallResponse = {
    "Ball": [],
}
frameResponses = {}
dirtyResponses = {}


def get_height_in_cm(movement):
    return (float(movement) * 21.59) / 0.05


def get_width_in_cm(movement):
    return (float(movement) * 21.59) / 0.03


def get_velocity(distance, time=4/30):
    return distance / time


def build_json_person(label, name, person_movement_index):
    ball = label['Geometry']
    ball_json = {
        "BoundingBox": ball["BoundingBox"],
        "Confidence": label["Confidence"],
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
    if len(bowlingBallResponse[name]) != 0:
        past_ball = bowlingBallResponse[name][person_movement_index]
        past_left = float(past_ball["BoundingBox"]["Left"])
        new_left = float(ball["BoundingBox"]["Left"])
        past_top = float(past_ball["BoundingBox"]["Top"])
        new_top = float(past_ball["BoundingBox"]["Top"])
        ball_json["movement"]["x"] = get_width_in_cm(new_left - past_left)
        ball_json["movement"]["y"] = get_width_in_cm(new_top - past_top)
        ball_json["movement"]["y_speed"] = get_velocity(abs(ball_json["movement"]["y"]))
        ball_json["movement"]["x_speed"] = get_velocity(abs(ball_json["movement"]["x"]))
        if len(bowlingBallResponse[name]) > 1:
            ball_json["past_speeds"]["x"] = past_ball["past_speeds"]["x"].copy()
            ball_json["past_speeds"]["x"].append(ball_json["movement"]["x_speed"])
            ball_json["past_speeds"]["y"] = past_ball["past_speeds"]["y"].copy()
            ball_json["past_speeds"]["y"].append(ball_json["movement"]["y_speed"])
            ball_json["movement"]["y_speed_avg"] = sum(ball_json["past_speeds"]["y"])/len(ball_json["past_speeds"]["y"])
            ball_json["movement"]["x_speed_avg"] = sum(ball_json["past_speeds"]["x"]) / len(ball_json["past_speeds"]["x"])
        else:
            ball_json["past_speeds"]["x"].append(ball_json["movement"]["x_speed"])
            ball_json["past_speeds"]["y"].append(ball_json["movement"]["y_speed"])
            ball_json["movement"]["y_speed_avg"] = ball_json["movement"]["y_speed"]
            ball_json["movement"]["x_speed_avg"] = ball_json["movement"]["x_speed"]

    return ball_json

#30FPS
person_movement_index = 0
for i in range(1, 259):
    # We are going to capture each half a second some information
    if i % 4 == 0:
        with open("frames/bowling_1/frame" + str(i) + ".jpg", 'rb') as source_image:
            response = client.detect_custom_labels(
                ProjectVersionArn="arn:aws:rekognition:us-east-2:996202008712:project/IA/version/IA.2021-05-16T18.09"
                                  ".36/1621210176628",
                Image={
                    'Bytes': source_image.read()
                }

            )
            frameResponses[i] = {}
            dirtyResponses[i] = {}
            for label in response['CustomLabels']:
                dirtyResponses[i][label['Name']] = {
                    'accuracy': label['Confidence'],
                    'name': label['Name'],
                    'instances': label['Geometry']
                }
                # We are going to pass only if there are any instances
                if "Geometry" in label and len(label['Geometry'].keys()) != 0 \
                        and "BoundingBox" in label['Geometry']:
                    # If it is a person it should be registered
                    if label['Name'] == "BowlBall":
                        bowl_json = build_json_person(label, "Ball", person_movement_index)
                        bowl_json["Frame"] = i
                        bowl_json["Second"] = i/30
                        bowlingBallResponse["Ball"].append(bowl_json)
                        if len(bowlingBallResponse["Ball"]) > 1:
                            person_movement_index += 1

                    frameResponses[i][label['Name']] = {
                        'accuracy': label['Confidence'],
                        'name': label['Name'],
                        'Geometry': label['Geometry']
                    }
with open('results/bowl_1/bowlTrack1.json', 'w') as outfile:
    json.dump(bowlingBallResponse, outfile, indent=4)
# with open('results/bowl_1/results.json', 'w') as outfile:
#     json.dump(frameResponses, outfile, indent=4)
# with open('results/bowl_1/dirty-results.json', 'w') as outfile:
#     json.dump(dirtyResponses, outfile, indent=4)
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
