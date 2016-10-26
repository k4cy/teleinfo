#!/usr/bin/env python
import time
import serial

ser = serial.Serial(
  port = '/dev/ttyUSB0',
  baudrate = 1200,
  parity = serial.PARITY_EVEN,
  stopbits = serial.STOPBITS_ONE,
  bytesize = serial.SEVENBITS,
  timeout = 1
)
doc = { }

resp = ser.readline()
while '\x02' not in resp:
  resp = ser.readline()
resp = ser.readline()
while '\x03' not in resp:
  resp = ser.readline()
  trame = resp.split()
  doc[trame[0]] = trame[1]

print doc