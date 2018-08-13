#!/usr/bin/env python
# -*- coding: utf-8 -*-
from time import gmtime, strftime
import serial
import time
ser = serial.Serial('COM4', 115200)

while 1:
 try:
  dado = ser.readline()
  print (dado)
  with open('dados.txt', 'a+') as arq: 
  #arq = open('dados.txt', 'w')
      data = strftime("%Y-%m-%d %H:%M:%S", gmtime())
      salvar = "" + data +"/"+ dado
      arq.writelines(salvar)
 except ser.SerialTimeoutException:
  print('Erro! dados não lidos...')
  time.sleep(1)
