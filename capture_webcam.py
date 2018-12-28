#!/usr/local/bin/python

import pymouse
import pykeyboard
import numpy
import time
import datetime
#import threading
import sys
import cv2
import os
import glob
import random


def remove_picture(image_folder, max_cnt_facial_image, max_cnt_non_facial_image):
    filelist = glob.glob(os.path.join(image_folder, "webcam-facial-*.png"))
    for f in filelist[0:] if max_cnt_facial_image == 0 else filelist[0:-max_cnt_facial_image]:
        os.remove(f)
    filelist = glob.glob(os.path.join(image_folder, "webcam-nonfacial-*.png"))
    for f in filelist[0:] if max_cnt_non_facial_image == 0 else filelist[0:-max_cnt_non_facial_image]:
        os.remove(f)
        
def take_picture(video_capture, pfolder="/Users/pcey334f/Desktop/Webcam"):
    if os.path.exists(pfolder) == False:
        print("Creating folder '", pfolder, "'")
        os.makedirs(pfolder)
    
    str_cur_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    filename = os.path.join(pfolder, "webcam{}.png".format(str_cur_time))
    
    # Capture frame-by-frame
    ret, frame = video_capture.read()
    # Display the resulting frame
    result = cv2.imwrite(filename, frame)
    return result, filename

# class MouseEventDetector(pymouse.PyMouseEvent):
#     def __init__(self):
#         super(self.__class__, self).__init__(self)
#         self.lock = threading.RLock()
#         self.clicked = False
#         self.moved = False
#         self.scrolled = False

#     def reset(self):
#         self.lock.acquire()
#         self.clicked = False
#         self.moved = False
#         self.scrolled = False
#         self.lock.release()
        
#     def click(self, x, y, button, press):
#         self.lock.acquire()
#         self.clicked = True
#         self.lock.release()
#         #time.sleep(0.1)

#     def move(self, x, y):
#         self.lock.acquire()
#         self.moved = True
#         self.lock.release()
#         #time.sleep(0.1)
        
#     def scroll(self, x, y, vertical, horizontal):
#         self.lock.acquire()
#         self.scrolled = True
#         self.lock.release()
#         #time.sleep(0.1)
        



image_folder = "/Users/pcey334f/Desktop/Webcam"
max_cnt_facial_image = 20
max_cnt_non_facial_image = 20

list_greeting = ["Hello.", "Welcome.", 'May I help you?']

try:
    #med = MouseEventDetector()
    #med.start()
    #print("Mouse event dectector started")

    face_cascade = cv2.CascadeClassifier('/usr/local/Cellar/opencv/2.4.13.2/share/OpenCV/haarcascades/haarcascade_frontalface_default.xml')
    print("Started face detector")
    
    video_capture = cv2.VideoCapture(0)
    print("Started video device")
    
    remove_picture(image_folder, 0, 0)
    print("Removed all previous captured images")

    print("Waiting for 5 seconds")
    time.sleep(5)

    m = pymouse.PyMouse()
    m_ppos = m.position() 
    while True:
        remove_picture(image_folder, max_cnt_facial_image, max_cnt_non_facial_image)

        # Detect mouse movement
        m_pos = m.position()
        m_delta_pos = numpy.array([m_pos[i] - m_ppos[i] for i in range(2)])
        m_mov_dis = numpy.sqrt(numpy.dot(m_delta_pos, m_delta_pos))
        m_ppos = m_pos

        if m_mov_dis > 0:
        #if med.moved == True or med.clicked == True or med.scrolled == True: 
            #print(med.moved, med.clicked, med.scrolled)
            #med.reset()

            # Get current time
            str_cur_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

            # Capture frame-by-frame
            ret, frame = video_capture.read()
            # Detect face
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            faces = face_cascade.detectMultiScale(
                        gray,
                        scaleFactor=1.1,
                        minNeighbors=5,
                        minSize=(30, 30),
                        flags = cv2.cv.CV_HAAR_SCALE_IMAGE
                    )
            # Draw a rectangle around the faces
            for (x, y, w, h) in faces:
                cv2.rectangle(frame, (x, y), (x+w, y+h), (0, 255, 0), 2)

            # Create directory if not found
            if os.path.exists(image_folder) == False:
                print("Creating folder '{}'".format(image_folder))
                os.makedirs(image_folder)

            if len(faces) > 0:
                os.system("google_speech -l en '{}' -e speed 0.9".format(random.sample(list_greeting, 1)[0]))
                filename = os.path.join(image_folder, "webcam-facial-{}.png".format(str_cur_time))
                time.sleep(1.5)
            else:
                filename = os.path.join(image_folder, "webcam-nonfacial-{}.png".format(str_cur_time))

            # Save to picture
            result = cv2.imwrite(filename, frame)
            if result:
                print("Created file '{}'".format(filename))

except KeyboardInterrupt:
    print("\nCtrl-C is entered. Quitting ...")
except Exception as e:
    print(e)
finally:
    # Ensure number of files are met
    remove_picture(image_folder, max_cnt_facial_image, max_cnt_non_facial_image)
    try:
        # When everything is done, release the capture
        video_capture.release()
    except:
        pass
    #try:
    #    # When everything is done, release the capture
    #    med.stop()
    #except:
    #    pass