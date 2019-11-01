#!/usr/bin/env python3


import time
from neopixel import *
import argparse
import random
from flask import Flask, abort, request
import sys
import json
import threading
import inspect
from concurrent.futures import ThreadPoolExecutor
import logging
import signal

logging.basicConfig(level="DEBUG")



# LED strip configuration:
LED_COUNT      = 476
LED_PIN        = 18      # GPIO pin connected to the pixels (18 uses PWM!).
#LED_PIN        = 10      # GPIO pin connected to the pixels (10 uses SPI /dev/spidev0.0).
LED_FREQ_HZ    = 800000  # LED signal frequency in hertz (usually 800khz)
LED_DMA        = 10      # DMA channel to use for generating signal (try 10)
LED_BRIGHTNESS = 255     # Set to 0 for darkest and 255 for brightest
LED_INVERT     = False   # True to invert the signal (when using NPN transistor level shift)
LED_CHANNEL    = 0       # set to '1' for GPIOs 13, 19, 41, 45 or 53



# Define functions which animate LEDs in various ways.
def colorWipe(strip, color, wait_ms=50):
    """Wipe color across display a pixel at a time."""
    for i in range(strip.numPixels()):
        strip.setPixelColor(i, color)
        strip.show()
        time.sleep(wait_ms/1000.0)

def solid(strip, color):
    """Set solid color"""
    for i in range(strip.numPixels()):
        strip.setPixelColor(i, color)
        strip.show()

def alternate(strip, color1, color2):
    """Set alternating colors"""
    for i in range(strip.numPixels()):
        if i % 2:
            strip.setPixelColor(i, color1)
        else:
            strip.setPixelColor(i, color2)
        strip.show()


def theaterChase(strip, color, wait_ms=50, iterations=10):
    """Movie theater light style chaser animation."""
    for j in range(iterations):
        for q in range(3):
            for i in range(0, strip.numPixels(), 3):
                strip.setPixelColor(i+q, color)
            strip.show()
            time.sleep(wait_ms/1000.0)
            for i in range(0, strip.numPixels(), 3):
                strip.setPixelColor(i+q, 0)

def wheel(pos):
    """Generate rainbow colors across 0-255 positions."""
    if pos < 85:
        return Color(pos * 3, 255 - pos * 3, 0)
    elif pos < 170:
        pos -= 85
        return Color(255 - pos * 3, 0, pos * 3)
    else:
        pos -= 170
        return Color(0, pos * 3, 255 - pos * 3)

def rainbow(strip, wait_ms=20, iterations=1):
    """Draw rainbow that fades across all pixels at once."""
    for j in range(256*iterations):
        for i in range(strip.numPixels()):
            strip.setPixelColor(i, wheel((i+j) & 255))
        strip.show()
        time.sleep(wait_ms/1000.0)

def rainbowCycle(strip, wait_ms=20, iterations=5):
    """Draw rainbow that uniformly distributes itself across all pixels."""
    for j in range(256*iterations):
        for i in range(strip.numPixels()):
            strip.setPixelColor(i, wheel((int(i * 256 / strip.numPixels()) + j) & 255))
        strip.show()
        time.sleep(wait_ms/1000.0)

def theaterChaseRainbow(strip, wait_ms=50):
    """Rainbow movie theater light style chaser animation."""
    for j in range(256):
        for q in range(3):
            for i in range(0, strip.numPixels(), 3):
                strip.setPixelColor(i+q, wheel((i+j) % 255))
            strip.show()
            time.sleep(wait_ms/1000.0)
            for i in range(0, strip.numPixels(), 3):
                strip.setPixelColor(i+q, 0)

def nightrider(strip, wait_ms=70):
    """ Knight raider kitt scanner """
    width = 5
    color = Color(0,127,0)
    while True:
        for i in range(strip.numPixels()):
            strip.setPixelColor(i, color)
            for w in range(width):
              strip.setPixelColor(i - w, dimColor(color))
            strip.show()
            time.sleep(wait_ms / 10000.0)
            strip.setPixelColor(i, 0)
            strip.setPixelColor(i - 5, 0)
        # reverse the direction
        for i in range(strip.numPixels() - 1, -1, -1):
            strip.setPixelColor(i, color)
            for w in range(width):
              strip.setPixelColor(i + w, dimColor(color))
            strip.show()
            time.sleep(wait_ms / 10000.0)
            strip.setPixelColor(i, 0)
            strip.setPixelColor(i + 5, 0)

def dimColor (color):
  """ Color is an 32-bit int that merges the values into one """
  return (((color&0xff0000)/3)&0xff0000) + (((color&0x00ff00)/3)&0x00ff00) + (((color&0x0000ff)/3)&0x0000ff)

def runBurst(strip):
    t = 0
    colorBurst = None
    colorBurst2 = None
    # colorParticle = ColorParticle(wheel(0), 30*2, strip.numPixels()/2, 2, 0, 0.05)
    while True:
        if t%40 == 0:
            colorBurst = ColorBurst(random.randint(0,strip.numPixels()-1), random.randint(0,255), 20, random.randint(20,35), 3, 0.05, 30/3, 30*1)
        if t == 0 or t%40 == 30:
            colorBurst2 = ColorBurst(random.randint(0,strip.numPixels()-1), random.randint(0,255), 20, random.randint(20,35), 3, 0.05, 30/3, 30*1)
        colors = [0]*strip.numPixels()
        setArrayValues(colors, colorBurst.simulate(t%40), False)
        setArrayValues(colors, colorBurst2.simulate((t-30)%40), False)
        # setArrayValues(colors, [colorParticle.simulate(t)], True)
        showColors(strip, colors)

        t+= 1
        time.sleep(1/60)

class ColorBurst:
    def __init__(self, center, hue, hueVariance, particleCount, maxVelocity, maxDrag, minFadeTime, maxFadeTime):
        self.center = center
        self.hue = hue
        self.hueVariance = hueVariance
        self.maxVelocity = maxVelocity
        self.maxDrag = maxDrag
        self.minFadeTime = minFadeTime
        self.maxFadeTime = maxFadeTime

        self.rng = random.Random()
        self.particles = [ColorParticle(wheel(hue + int(self.rng.uniform(-hueVariance, hueVariance))), self.rng.uniform(minFadeTime, maxFadeTime), center, self.rng.uniform(-maxVelocity, maxVelocity),
            0, self.rng.uniform(0, maxDrag)) for i in range(particleCount)]

    def __repr__(self):
        return "{},{},{},{}".format(self.center, self.color, self.maxVelocity, self.fadeTime)

    # returns array of (index,color)
    def simulate(self, t):
        return [p.simulate(t) for p in self.particles]

class ColorParticle:
    def __init__(self, color, fadeTime, position, velocity, acceleration, drag):
        self.color = color
        self.fadeTime = fadeTime
        self.position = position
        self.velocity = velocity
        self.acceleration = acceleration
        self.drag = drag

    def __repr__(self):
        return "{},{},{},{}".format(self.color, self.position, self.velocity, self.drag)

    # returns (index, color)
    def simulate(self, t):
        # return (int(self.position + self.velocity*t + self.acceleration/2*t**2 -
            # (self.velocity*t**2)*self.drag*t),
            # 0),
        position = int(self.position + (self.velocity*(1/(1+self.drag*t)))*t)
        color = Color(0,0,0)
        if t >= 0:
            color = Color(max(0, int(lerp(getRed(self.color), 0, t/self.fadeTime))),
                max(0, int(lerp(getGreen(self.color), 0, t/self.fadeTime))),
                max(0, int(lerp(getBlue(self.color), 0, t/self.fadeTime))))
        return (position, color)

def lerp(a,b,t):
    return (b-a)*t + a


def colorBursts(strip, wait_ms=10):
	for i in range(256):
		center = random.randint(0, strip.numPixels())
		colorBegin = random.randint(0, 256)
		for j in range(strip.numPixels()):
			strip.setPixelColor((center+j)%strip.numPixels(), wheel((colorBegin + 5*j)%256))
			strip.setPixelColor((center-j)%strip.numPixels(), wheel((colorBegin + 5*j)%256))
			strip.show()
			time.sleep(wait_ms/1000.0)

# takes array, and array of (index, value) and sets appropriately
def setArrayValues(arr, vals, wrap):
    for val in vals:
        if wrap or (val[0] >= 0 and val[0] < len(arr)):
            arr[val[0]%len(arr)] = val[1]

def makeColorBurst(pixels, center, color, maxVelocity, fadeTime, t):
    random.jumpahead
    colors = [0]*pixels.numPixels()
    # colors[(center t/fadeTime
    for j in range(strip.numPixels()):
        strip.setPixelColor((center+j)%strip.numPixels(), wheel((colorBegin + 5*j)%256))
        strip.setPixelColor((center-j)%strip.numPixels(), wheel((colorBegin + 5*j)%256))
        strip.show()
        time.sleep(wait_ms/1000.0)

    return colors

def getRed(color):
    return (color >> 16) & (2**16 - 1)

def getGreen(color):
    return (color >> 8) & (2**8 - 1)

def getBlue(color):
    return color & (2**8 - 1)

def getWhite(color):
    return (color >> 24) & (2**24 - 1)

def showColors(pixels, colors):
    for i in range(min(pixels.numPixels(), len(colors))):
        pixels.setPixelColor(i, colors[i])

    pixels.show()


class LED(object):

    def __init__(self):
        self.strip = Adafruit_NeoPixel(LED_COUNT, LED_PIN, LED_FREQ_HZ, LED_DMA, LED_INVERT, LED_BRIGHTNESS, LED_CHANNEL)
        self.strip.begin()

        self.pattern = None
        self.led_on = None

        # self.thread = threading.Thread()
        self.pool = ThreadPoolExecutor(1)
        self.stopper = threading.Event()
        self.future = None

        signal.signal(signal.SIGALRM, self.stop_pattern)




    def get_status(self):
        return {"pattern": self.pattern, "led_on": self.led_on}

    def stop_pattern(self, num, stack):
        print("Received SIGALRM")
        raise Exception("STOP")

    def run_pattern(self, pattern):
        currentstatus = self.get_status()
        if currentstatus['pattern'] == pattern:
            print("Already running: {}".format(pattern))
            return True
        if pattern not in dir(self):
            print("Pattern not found.")
            return False

        print("Sending pattern: {}".format(pattern))
        func = getattr(self, pattern)

        try:
            if self.future.running():
                self.stopper.set()
                signal.alarm(1)
                logging.debug("Stopping current process.")
                while not self.future.done():
                    logging.debug("Waiting for current pattern to stop.")
                    time.sleep(0.5)
        except Exception as e:
            logging.debug(e)

        self.stopper.clear()
        self.future = self.pool.submit(func)


        #logging.debug("Result: {}".format(future.result()))
        #
        #     print("Currently running {}...stopping.".format(self.pattern))
        #     self.stopper.set()
        #     self.thread.join(5)
        # self.thread = threading.currentThread(target=func)
        # self.thread.start()



    def test(self):
        self.pattern = inspect.stack()[0][3]
        self.led_on = True
        try:
            print('Color wipe animations.')
            colorWipe(self.strip, Color(255, 0, 0))  # Red wipe
            colorWipe(self.strip, Color(0, 255, 0))  # Blue wipe
            colorWipe(self.strip, Color(0, 0, 255))  # Green wipe
            print('Theater chase animations.')
            theaterChase(self.strip, Color(127, 127, 127))  # White theater chase
            theaterChase(self.strip, Color(127, 0, 0))  # Red theater chase
            theaterChase(self.strip, Color(0, 0, 127))  # Blue theater chase
            print('Rainbow animations.')
            rainbow(self.strip)
            rainbowCycle(self.strip)
            theaterChaseRainbow(self.strip)
        except Exception as ex:
            logging.debug("Stopped.")
        finally:
            signal.alarm(0)

    def theater(self):
        self.pattern = inspect.stack()[0][3]
        self.led_on = True
        while not self.stopper.is_set():
            print('Theater chase animations.')
            theaterChase(self.strip, Color(127, 127, 127))  # White theater chase
            theaterChase(self.strip, Color(127, 0, 0))  # Red theater chase
            theaterChase(self.strip, Color(0, 0, 127))  # Blue theater chase

    def rainbow(self):
        self.pattern = inspect.stack()[0][3]
        self.led_on = True
        while not self.stopper.is_set():
            print('Rainbow')
            rainbowCycle(self.strip)
        return True

    def red(self):
        self.pattern = inspect.stack()[0][3]
        self.led_on = True
        print("Setting red.")
        solid(self.strip, Color(0,255,0))

    def blue(self):
        self.pattern = inspect.stack()[0][3]
        self.led_on = True
        print("Setting blue.")
        solid(self.strip, Color(0,0,255))

    def white(self):
        self.pattern = inspect.stack()[0][3]
        self.led_on = True
        self.pattern = "white"
        self.led_on = True
        print("Setting white.")
        solid(self.strip, Color(127,127,127))
        return True

    def christmas(self):
        self.pattern = inspect.stack()[0][3]
        self.led_on = True
        print("Setting christmas.")
        alternate(self.strip,Color(0,255,0),Color(255,0,0))

    def nicaragua(self):
        self.pattern = inspect.stack()[0][3]
        self.led_on = True
        print("Setting nicaragua.")
        alternate(self.strip,Color(0,0,127),Color(127,127,127))

    def canada(self):
        self.pattern = inspect.stack()[0][3]
        self.led_on = True
        print("Setting canada.")
        alternate(self.strip,Color(0,127,0),Color(127,127,127))

    def burst(self):
        self.pattern = inspect.stack()[0][3]
        self.led_on = True
        print("Setting burst.")
        t = 0
        colorBurst = None
        colorBurst2 = None
        # colorParticle = ColorParticle(wheel(0), 30*2, strip.numPixels()/2, 2, 0, 0.05)
        while not self.stopper.is_set():
            if t % 40 == 0:
                colorBurst = ColorBurst(random.randint(0, self.strip.numPixels() - 1), random.randint(0, 255), 20,
                                        random.randint(20, 35), 3, 0.05, 30 / 3, 30 * 1)
            if t == 0 or t % 40 == 30:
                colorBurst2 = ColorBurst(random.randint(0, self.strip.numPixels() - 1), random.randint(0, 255), 20,
                                         random.randint(20, 35), 3, 0.05, 30 / 3, 30 * 1)
            colors = [0] * self.strip.numPixels()
            setArrayValues(colors, colorBurst.simulate(t % 40), False)
            setArrayValues(colors, colorBurst2.simulate((t - 30) % 40), False)
            # setArrayValues(colors, [colorParticle.simulate(t)], True)
            showColors(self.strip, colors)

            t += 1
            time.sleep(1 / 60)

    def knightrider(self):
        self.pattern = inspect.stack()[0][3]
        self.led_on = True
        print("Knight rider!!")
        nightrider(self.strip)

    def off(self):
        self.pattern = inspect.stack()[0][3]
        self.led_on = False
        for i in range(1, 3):
            colorWipe(self.strip, Color(0, 0, 0), 10)
        return True




app = Flask(__name__)
@app.route('/status')
def status():
    currentstatus = LEDStrip.get_status()
    print("Status: {}".format(currentstatus))
    return json.dumps(currentstatus)

@app.route('/setpattern', methods=['POST'])
def setpattern():
    reqjson = request.get_json()
    print("Request: {}".format(reqjson))

    try:
        pattern = reqjson['pattern']

    except Exception as e:
        print(e)
        abort(400)

    print("Setting pattern to: {}".format(pattern))
    pattern_result = LEDStrip.run_pattern(pattern)
    result = {"result": pattern_result, "pattern": pattern}
    return json.dumps(result)



# Main program logic follows:
if __name__ == '__main__':
    # Process arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--test', action='store_true', help='Run strand test and exit.')
    parser.add_argument('-c', '--clear', action='store_true', help='Clear strip')
    args = parser.parse_args()

    if args.test:
        print('Press Ctrl-C to quit.')

        strip = Adafruit_NeoPixel(LED_COUNT, LED_PIN, LED_FREQ_HZ, LED_DMA, LED_INVERT, LED_BRIGHTNESS, LED_CHANNEL)
        strip.begin()

        try:
            while True:
                print('Color wipe animations.')
                colorWipe(strip, Color(255, 0, 0))  # Red wipe
                colorWipe(strip, Color(0, 255, 0))  # Blue wipe
                colorWipe(strip, Color(0, 0, 255))  # Green wipe
                print('Theater chase animations.')
                theaterChase(strip, Color(127, 127, 127))  # White theater chase
                theaterChase(strip, Color(127, 0, 0))  # Red theater chase
                theaterChase(strip, Color(0, 0, 127))  # Blue theater chase
                print('Rainbow animations.')
                rainbow(strip)
                rainbowCycle(strip)
                theaterChaseRainbow(strip)

        except KeyboardInterrupt:

            colorWipe(strip, Color(0, 0, 0), 10)
            sys.exit(0)

    elif args.clear:
        print("Clearing strip.")
        strip = Adafruit_NeoPixel(LED_COUNT, LED_PIN, LED_FREQ_HZ, LED_DMA, LED_INVERT, LED_BRIGHTNESS, LED_CHANNEL)
        strip.begin()
        colorWipe(strip, Color(0, 0, 0), 10)
        sys.exit(0)


    else:
        LEDStrip = LED()
        LEDStrip.run_pattern("off")
        app.run()
