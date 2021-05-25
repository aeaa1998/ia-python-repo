# Program To Read video
# and Extract Frames
import cv2


# Function to extract frames
def FrameCapture(path, folder):
    # Path to video file
    vidObj = cv2.VideoCapture(path)

    # Used as counter variable
    count = 0

    # checks whether frames were extracted
    success = 1

    while success:
        # vidObj object calls read
        # function extract frames
        success, image = vidObj.read()

        # Saves the frames with frame-count
        if image is not None:
            cv2.imwrite(f"frames/{folder}/frame%d.jpg" % count, image)
            count += 1

# FrameCapture("cut1.mp4", "pp")
# FrameCapture("bowl_1.mov", "bowling_1")
FrameCapture("bolos1.mp4", "bowling_2")
#17-137