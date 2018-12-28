from AppKit import NSApplication, NSApp
from Foundation import NSObject, NSLog
from Cocoa import (NSEvent, 
  NSEventTypeKeyDown,
  NSKeyDown, NSKeyDownMask, 
  NSKeyUp, NSKeyUpMask,
  NSLeftMouseDownMask, NSLeftMouseUpMask, NSLeftMouseDraggedMask,
  NSRightMouseDownMask, NSRightMouseUpMask, NSRightMouseDraggedMask, 
  NSMouseMovedMask, NSScrollWheelMask,
  NSSystemDefined,
  NSFlagsChangedMask)
from PyObjCTools import AppHelper
import signal
import os
import numpy

list_greeting = ["Hello.", "Welcome.", 'May I help you?']

class AppDelegate(NSObject):
    def applicationDidFinishLaunching_(self, notification):
        mask = (NSEventTypeKeyDown
                | NSKeyDown
                | NSKeyDownMask
                | NSKeyUp
                | NSKeyUpMask
                | NSLeftMouseDownMask
                | NSLeftMouseUpMask
                | NSLeftMouseDraggedMask
                | NSRightMouseDownMask
                | NSRightMouseUpMask
                | NSRightMouseDraggedMask
                | NSMouseMovedMask
                | NSScrollWheelMask 
                | NSSystemDefined 
                | NSFlagsChangedMask)
        mask = int('0xFFFFFF', 16)
        mask = numpy.bitwise_xor(mask, NSKeyUpMask)
        print(bin(mask))
        NSEvent.addGlobalMonitorForEventsMatchingMask_handler_(mask, handler)
        #NSEvent.addLocalMonitorForEventsMatchingMask_handler_(mask, handler)

def handler(event):
    try:
        print("handler", event)
        
        #os.system("google_speech -l en 'hello' -e speed 0.9")
        
    except (SystemExit, KeyboardInterrupt):
        AppHelper.stopEventLoop()

def main():
  try:
    app = NSApplication.sharedApplication()
    delegate = AppDelegate.alloc().init()
    NSApp().setDelegate_(delegate)
    
    def handler(signal, frame):
      print("\nSending stopEventLoop in run function sub function handler")
      AppHelper.stopEventLoop()
    
    signal.signal(signal.SIGINT, handler)

    AppHelper.runEventLoop()

  except (SystemExit, KeyboardInterrupt):
    print("\nCtrl-C is entered")
    AppHelper.stopEventLoop()


if __name__ == '__main__':
    main()