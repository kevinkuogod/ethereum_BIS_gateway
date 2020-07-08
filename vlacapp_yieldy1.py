#!/usr/bin/python
import paho.mqtt.client as mqtt
import simplejson as json
import pymongo
import os
import ipfshttpclient
import numpy as np
import time
import ssl
from bson.objectid import ObjectId
from web3.auto import w3
import hmac
import hashlib
import json
import subprocess
import threading
import math

#------------------------------------------init-------------------------------------------------------
connected = w3.isConnected()
print(connected)

#connect db
pymongo_client = pymongo.MongoClient("localhost",27017)
#choose db
db = pymongo_client.iotsensordata
#choose ethereum geth peer
contrace_address = ''
greeter = ''
txnObject = ''
if connected and w3.clientVersion.startswith('Geth'):
  print('Connect geth success')
  contrace_address = w3.toChecksumAddress("0x41071ddb254d0f0972ef884216239c229307a7cd")
  greeter = w3.eth.contract(
      address=contrace_address,
      abi='[{"constant":false,"inputs":[{"internalType":"string","name":"_asseid","type":"string"}],"name":"testfun","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"}]'
  )
  txnObject = {'from': w3.eth.accounts[0],'gas': 100000}
else:
  print('Connect false')

#export WEB3_PROVIDER_URI=http://192.168.0.75:8545

ipfs_client = ipfshttpclient.connect('/ip4/127.0.0.1/tcp/5001')

# if(os.path.isfile("hash_recode_sub_yieldy1.txt")):
#   os.remove("hash_recode_sub_yieldy1.txt")
# hash_recode = open("hash_recode_sub_yieldy1.txt", "w+")


#yieldy1 tea1
# sensor_id_array = [['Z001','C001']]
# sensor_id_login = [[0,0]]
# sensor_ldr_id_array = ['Z001']
# sensor_camera_id_array = ['C001']
# sensor_group_list=['ldr','camera']
# asset_RFID_list=['tea1']
# sensor_camera_array = [[]]
# sensor_ldr_array = [[]]
# video_ipfs_hash_array = [""]
# login_logout_recode_array = []
# sensor_ethereum_blockchain_tx_array = [[]]
# sensor_group_login_time_array = [-1]
# asset_trace_asset_in_leagersite_id_array = ['5ebbf3aa4346b9369fde9672']
# leager_site_id='5ebbe7949f78fc20bab75a86'
# sensor_ldr_group=[['ZOO1']]
# sensor_camera_group=[['COO1']]

#yieldy2 Desiccant
# sensor_id_array = [['Z002','C002']]
# sensor_ldr_id_array = ['Z002']
# sensor_camera_id_array = ['C002']
# sensor_group_list=['ldr','camera']
# asset_RFID_list=['Desiccant1']
# sensor_camera_array = [[]]
# sensor_ldr_array = [[]]
# video_ipfs_hash_array = [""]
# login_logout_recode_array = []
# sensor_ethereum_blockchain_tx_array = [[]]
# sensor_id_login = [[0,0]]
# sensor_group_login_time_array = [-1]
# asset_trace_asset_in_leagersite_id_array = ['5ec8e9af593354bbcb82d366']
# leager_site_id='5ebbe7949f78fc20bab75a87'
# sensor_ldr_group=[['ZOO2']]
# sensor_camera_group=[['COO2']]

#yieldy3 wrapper
# sensor_id_array = [['Z003','C003']]
# sensor_ldr_id_array = ['Z003']
# sensor_camera_id_array = ['C003']
# sensor_group_list=['ldr','camera']
# asset_RFID_list=['wrapper1']
# sensor_camera_array = [[]]
# sensor_ldr_array = [[]]
# video_ipfs_hash_array = [""]
# login_logout_recode_array = []
# sensor_ethereum_blockchain_tx_array = [[]]
# sensor_id_login = [[0,0]]
# sensor_group_login_time_array = [-1]
# asset_trace_asset_in_leagersite_id_array = ['5ec8e9cb88107cacdd85ae60']
# leager_site_id='5ebbe7949f78fc20bab75a88'
# sensor_ldr_group=[['ZOO3']]
# sensor_camera_group=[['COO3']]

#manufacture tea1 Desiccant1 wrapper1
sensor_id_array = [['Z001','C001'],['Z002','C002'],['Z003','C003']]
sensor_ldr_id_array = ['Z001','Z002','Z003']
sensor_camera_id_array = ['C001','C002','C003']
sensor_group_list=['ldr','camera']
asset_RFID_list=['tea1','Desiccant1','wrapper1']
sensor_camera_array = [[],[],[]]
sensor_ldr_array = [[],[],[]]
video_ipfs_hash_array = ["","",""]
login_logout_recode_array = []
sensor_ethereum_blockchain_tx_array = [[],[],[]]
sensor_id_login = [[0,0],[0,0],[0,0]]
sensor_group_login_time_array = [-1,-1,-1]
asset_trace_asset_in_leagersite_id_array = ['5ebbf3aa4346b9369fde9672','5ec8e9af593354bbcb82d366','5ec8e9cb88107cacdd85ae60']
leager_site_id='5ebbe7949f78fc20bab75a89'
sensor_ldr_group=[['ZOO1'], ['ZOO2'], ['ZOO3']]
sensor_camera_group=[['COO1'], ['COO2'], ['COO3']]

#delivery_place tea1
# sensor_id_array = [['Z001','C001']]
# sensor_id_login = [[0,0]]
# sensor_ldr_id_array = ['Z001']
# sensor_camera_id_array = ['C001']
# sensor_group_list=['ldr','camera']
# asset_RFID_list=['tea1']
# sensor_camera_array = [[]]
# sensor_ldr_array = [[]]
# video_ipfs_hash_array = [""]
# login_logout_recode_array = []
# sensor_ethereum_blockchain_tx_array = [[]]
# sensor_group_login_time_array = [-1]
# asset_trace_asset_in_leagersite_id_array = ['5ebbf3aa4346b9369fde9672']
# leager_site_id='5ebbe7949f78fc20bab75a8a'
# sensor_ldr_group=[['ZOO1']]
# sensor_camera_group=[['COO1']]

#retailer tea1
# sensor_id_array = [['Z001','C001']]
# sensor_id_login = [[0,0]]
# sensor_ldr_id_array = ['Z001']
# sensor_camera_id_array = ['C001']
# sensor_group_list=['ldr','camera']
# asset_RFID_list=['tea1']
# sensor_camera_array = [[]]
# sensor_ldr_array = [[]]
# video_ipfs_hash_array = [""]
# login_logout_recode_array = []
# sensor_ethereum_blockchain_tx_array = [[]]
# sensor_group_login_time_array = [-1]
# asset_trace_asset_in_leagersite_id_array = ['5ebbf3aa4346b9369fde9672']
# leager_site_id='5ebbe7949f78fc20bab75a8b'
# sensor_ldr_group=[['ZOO1']]
# sensor_camera_group=[['COO1']]

#buyer tea1
# sensor_id_array = [['Z001','C001']]
# sensor_id_login = [[0,0]]
# sensor_ldr_id_array = ['Z001']
# sensor_camera_id_array = ['C001']
# sensor_group_list=['ldr','camera']
# asset_RFID_list=['tea1']
# sensor_camera_array = [[]]
# sensor_ldr_array = [[]]
# video_ipfs_hash_array = [""]
# login_logout_recode_array = []
# sensor_ethereum_blockchain_tx_array = [[]]
# sensor_group_login_time_array = [-1]
# asset_trace_asset_in_leagersite_id_array = ['5ebbf3aa4346b9369fde9672']
# leager_site_id='5ebbe7949f78fc20bab75a8c'
# sensor_ldr_group=[['ZOO1']]
# sensor_camera_group=[['COO1']]


mqtt_sensor_channel=[
  "publish/ldr/data/",
  "publish/camera/data/",
]

mqtt_service_channel=[
  "sensor_login",
  "sensor_data",
  "sensor_logout",
]

#use in test
count_iot_data_number=[0,0,0]

#trigger with write txt
# time_to_write_txt=[0]
end=[0]
calculator_time=[0]
calculator_time_group_string=['']
mqtt_client = mqtt.Client()
calculator_Sub_run_time_Thread=""

#  if(time_to_write_txt[0] == 1):

#---------------------------------------------mqtt service--------------------------------------------
def on_BrokerConnect(client, userdata, flags, rc):
  print("Connect with brocker "+ str(rc))
  global mqtt_sensor_channel,mqtt_service_channel
  global sensor_id_array
  for count_mqtt_sensor_channel in range(len(mqtt_sensor_channel)):
    for count_sensor_id_array in range(len(sensor_id_array)):
        client.subscribe(mqtt_sensor_channel[count_mqtt_sensor_channel]+str(count_sensor_id_array), qos=2)
    for count_mqtt_service_channel in range(len(mqtt_service_channel)):
      client.subscribe(mqtt_service_channel[count_mqtt_service_channel], qos=2)

def on_BrokerMessage(client, userdata, msg):
  global mqtt_service_channel
  global sensor_id_login,sensor_group_login_time_array,switch_site_logout,lock_start_login
  global sensor_camera_array,sensor_ldr_array,sensor_id_array,video_ipfs_hash_array
  global count_iot_data_number
  global ipfs_client

  print("take message")
  #print(msg.topic)
  json_data = json.loads(msg.payload.decode('utf-8'))
  #print(json_data)
  if(msg.topic == (mqtt_service_channel[0])):
    print('into sensor login------------------------------------')
    print(json_data)
    #print(sensor_id_array)
    #print(int(json_data['determine_data_group']))
    count_sensor_id_login_array = 0
    for sensor_id in sensor_id_array[int(json_data['determine_data_group'])]:
      print('check sensor')
      if(json_data['asset_sensor_RFID'] == sensor_id):
        print('sensor:'+sensor_id)
        print('start_time:'+str(int(json_data['start_time'])))
        sensor_id_login[int(json_data['determine_data_group'])][count_sensor_id_login_array] = 1
      count_sensor_id_login_array+=1
    print('test login end')
    print((np.cumsum(sensor_id_login[json_data['determine_data_group']])[len(sensor_id_login[json_data['determine_data_group']])-1]) )
    if((np.cumsum(sensor_id_login[json_data['determine_data_group']])[len(sensor_id_login[json_data['determine_data_group']])-1]) == 1):
      sensor_group_login_time_array[json_data['determine_data_group']] = time.time()
    if(((np.cumsum(sensor_id_login[json_data['determine_data_group']])[len(sensor_id_login[json_data['determine_data_group']])-1]) == len(sensor_id_login[json_data['determine_data_group']])) and
        lock_start_login != 2
    ):
      lock_start_login = 1
    
  if(msg.topic == (mqtt_service_channel[1])):
    print('test_into_sensor')
    #need decode to string
    #print(msg.payload.decode('utf-8'))
    #json_data = msg.payload.decode('utf-8')
    #print(json_data)
    #camera_data
    # print(json_data)
    if(json_data['sensor_type'] == 'camera'):
      #print('recode camera data')
      sensor_camera_array[int(json_data['determine_data_group'])].append(json_data)
      count_iot_data_number[0]+=1
      print('store camera data')
    if(json_data['sensor_type'] == 'ldr'):
      #sensor_ldr_array.append(json_data['sensor_ldr_array'])
      #print('recode ldr data')
      sensor_ldr_array[int(json_data['determine_data_group'])].append(json_data)
      count_iot_data_number[1]+=1
      print('store ldr data')
    #print(sensor_ldr_array[0]['determine_data_ori'])
    #process_raw_data(int(int(json_data['determine_data_group']))
  
  if(msg.topic == (mqtt_service_channel[2])):
    print('senesor:logut------------------------------------')
    print(json_data)
    #print(json_data)
    count_sensor_id_login_array = 0
    #sensor logout check
    for sensor_id in sensor_id_array[int(json_data['determine_data_group'])]:
      print('cur:'+sensor_id)
      print('find0:'+json_data['asset_sensor_RFID'])
      if(json_data['asset_sensor_RFID'] == sensor_id):
        #print('find1:'+json_data['asset_sensor_RFID'][0])
        if(json_data['sensor_type'] == 'ldr'):
          print('senesor:'+sensor_id+',time:'+str(time.strftime("%Y-%m-%d %H:%M:%S")))
          sensor_id_login[json_data['determine_data_group']][count_sensor_id_login_array] = 0
          #print('logut')
          break
        if(json_data['sensor_type'] == 'camera'):
          #print('find2:'+json_data['asset_sensor_RFID'][0])
          print('senesor:'+sensor_id+',time:'+str(time.strftime("%Y-%m-%d %H:%M:%S")))
          #connect ipfs
          #print('test ipfs process0')
          #print(client)
          #print('test ipfs process1')
          #p=subprocess.Popen("ipfs add "+json_data['video_path'], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
          #(stdoutput,erroutput) = p.communicate() 
          #print(stdoutput.decode().split(' ')[1])
          print(json_data['video_path'])
          res = ipfs_client.add(json_data['video_path'])
          #print('test ipfs process2')
          print(res)
          print(res['Hash'])
          print('video end')
          #video_ipfs_hash_array.append(stdoutput.decode().split(' ')[1])
          video_ipfs_hash_array[json_data['determine_data_group']]=res['Hash']
          sensor_id_login[json_data['determine_data_group']][count_sensor_id_login_array] = 0
          #print('logut')
          break
      count_sensor_id_login_array+=1
    if((np.cumsum(sensor_id_login[json_data['determine_data_group']])[len(sensor_id_login[json_data['determine_data_group']])-1]) == 0):
      switch_site_logout[json_data['determine_data_group']] = 1
    #clear and sync data to DB
    print('01test switch_site data')

raw_data_wait_time=0
process_raw_data_tmp_group=[]
determine_data_group_task=0
def process_raw_data():
  global raw_data_wait_time,determine_data_group_task
  global process_raw_data_tmp_group
  while True:
    for sensor_group_number in range(len(sensor_camera_array)):
      while((len(sensor_ldr_array[sensor_group_number]) > 0) and (len(sensor_camera_array[sensor_group_number]) > 0)):
        sync_msg = sync_sensor_data(sensor_group_number)
        if(raw_data_wait_time == 0):
          sensor_pos=[0]
          sensor_id=['']
          if(sync_msg == 1):
            print('sync ok')
            determine_data_group_task = sensor_ldr_array[sensor_group_number][0]['determine_data_group']
          if(sync_msg == 2):
            print('lost camera')
            #camera error
            determine_data_group_task = sensor_ldr_array[sensor_group_number][0]['determine_data_group']

            for sensor in sensor_group_list:
              if(sensor == 'camera'):
                break
              sensor_pos[0] = sensor_pos[0]+1 
            sensor_id[0] = sensor_id_array[determine_data_group_task][sensor_pos[0]]

          if(sync_msg == 3):
            print('lost ldr')
            #ldr error
            determine_data_group_task = sensor_camera_array[sensor_group_number][0]['determine_data_group']
            
            sensor_pos=[0]
            sensor_id=['']
            for sensor in sensor_group_list:
              if(sensor == 'ldr'):
                break
              sensor_pos[0] = sensor_pos[0]+1
            sensor_id[0] = sensor_id_array[determine_data_group_task][sensor_pos[0]]
          mongodb_insert_id = sent_sensor_data_to_DB(sensor_group_number,sync_msg, sensor_id[0])
          sensor_ldr_total_value_hash, yolo_tag_total_value, frame_hash_total_value = hash_sensor_data(sync_msg,sensor_group_number)
          sent_data_to_blockchain(sensor_ldr_total_value_hash, yolo_tag_total_value, frame_hash_total_value, determine_data_group_task, mongodb_insert_id)
          pop_sensor_data(sensor_group_number)
        elif(raw_data_wait_time == 1):
          time.sleep(12)
          print('case packet order')
          #需要排已經收到的大小
          for sensor_camera_data_ori in sensor_camera_array[sensor_group_number]:
            for sensor_camera_data_cmp in sensor_camera_array[sensor_group_number]:
              if(sensor_camera_data_ori['packet_number'] > sensor_camera_data_cmp['packet_number']):
                process_raw_data_tmp_group.append(sensor_camera_data_ori)
                sensor_camera_data_ori = sensor_camera_data_cmp
                sensor_camera_data_cmp = process_raw_data_tmp_group[0]
                process_raw_data_tmp_group.pop(0)
          for sensor_ldr_data_ori in sensor_ldr_array[sensor_group_number]:
            for sensor_ldr_data_cmp in sensor_ldr_array[sensor_group_number]:
              if(sensor_ldr_data_ori['packet_number'] > sensor_ldr_data_cmp['packet_number']):
                process_raw_data_tmp_group.append(sensor_ldr_data_ori)
                sensor_ldr_data_ori = sensor_ldr_data_cmp
                sensor_ldr_data_cmp = process_raw_data_tmp_group[0]
                process_raw_data_tmp_group.pop(0)
          print('order camera result-----------------------------------------------------------------')
          print(sensor_camera_array[sensor_group_number])
          print('order ldr result-----------------------------------------------------------------')
          print(sensor_ldr_array[sensor_group_number])


def _generate_signature(data):
  return hmac.new('kevin'.encode('utf-8'), data.encode('utf-8'), hashlib.sha256).hexdigest()

def sync_sensor_data(sensor_group_number):
  global raw_data_wait_time

  check_find_data = 0
  if(sensor_ldr_array[sensor_group_number][0]['packet_number'] != sensor_camera_array[sensor_group_number][0]['packet_number']):
    if(raw_data_wait_time != 0):
      if(sensor_ldr_array[sensor_group_number][0]['packet_number'] > sensor_camera_array[sensor_group_number][0]['packet_number']):
        for sensor_camera_data in sensor_camera_array[sensor_group_number]:
          if(sensor_ldr_array[sensor_group_number][0]['packet_number'] > sensor_camera_array[sensor_group_number][0]['packet_number']):
            if(len(sensor_camera_array[sensor_group_number]) > 0):
              print('case camera pop------------------------------------------------------')
              sensor_camera_array.pop(0)
            else:
              check_find_data = 1
              break
        if(check_find_data != 1):
          check_find_data = 2
      elif(sensor_ldr_array[sensor_group_number][0]['packet_number'] < sensor_camera_array[sensor_group_number][0]['packet_number']):
        for sensor_ldr_data in sensor_ldr_array[sensor_group_number]:
          if(sensor_ldr_array[sensor_group_number][0]['packet_number'] < sensor_camera_array[sensor_group_number][0]['packet_number']):
            if(len(sensor_ldr_array[sensor_group_number]) > 0):
              sensor_ldr_array[sensor_group_number].pop(0)
              print('case ldr pop--------------------------------------------------------------')
            else:
              check_find_data = 1
              break
        if(check_find_data != 1):
          check_find_data = 3
      raw_data_wait_time = 0
    else:
      raw_data_wait_time = 1
  else:
    check_find_data = 1
    raw_data_wait_time = 0
  return check_find_data


def pop_sensor_data(sensor_group_number):
  #print(realtime_data_id.inserted_id)
  #pop(0) will remove list 0 pos 
  sensor_camera_array[sensor_group_number].pop(0)
  sensor_ldr_array[sensor_group_number].pop(0)

def sent_data_to_blockchain(sensor_ldr_total_value_hash, yolo_tag_total_value, frame_hash_total_value, determine_data_group, mongodb_insert_id):
  tx_json={}
  tx_json['asset_ID']=asset_RFID_list[determine_data_group]
  tx_json['sensor_idr_total_value_hash']=sensor_ldr_total_value_hash
  tx_json['yolo_tag_total']=yolo_tag_total_value
  tx_json['frame_hash_total']=frame_hash_total_value
  tx_json['mongodb_insert_id']=str(mongodb_insert_id)
  tx_json = json.dumps(tx_json)
  print(tx_json)
  #ethereum blockchain
  determine_account_agree = w3.geth.personal.unlockAccount(w3.eth.accounts[0], '1234')
  use_contract_result = greeter.functions.testfun(tx_json).transact(txnObject)
  print(use_contract_result.hex())
  print('smile2')
  sensor_ethereum_blockchain_tx_array[determine_data_group].append(use_contract_result.hex())

#  os.system('node python_to_js_test.js sensor_ldr_array_tmp[0]['asset_RFID'] sensor_ldr_total_value_hash \
#  yolo_tag_total_value frame_hash_total_value
#  ')

def hash_sensor_data(sync_msg,sensor_group_number):
  #hash data
  sensor_ldr_total_value = -1
  yolo_tag_total_value = ''
  frame_hash_total_value = ''
  if(sync_msg != 3):
    for sensor_ldr_data in sensor_ldr_array[sensor_group_number][0]['sensor_value']:
      sensor_ldr_total_value  = sensor_ldr_total_value+int(sensor_ldr_data)
  sensor_ldr_total_value_hash = hashlib.sha256((str(sensor_ldr_total_value).encode()))

  #print('hash sensor_ldr_total_value success')
  if(sync_msg != 2):
    for yolo_tag_data in sensor_camera_array[sensor_group_number][0]['yolo_tag_array']:
      yolo_tag_total_value  = yolo_tag_total_value+yolo_tag_data
  yolo_tag_total_value_hash = hashlib.sha256((yolo_tag_total_value.encode()))
  #print('hash yolo_tag_total_value success')
  if(sync_msg != 2):
    for frame_hash_data in sensor_camera_array[sensor_group_number][0]['frame_hash_array']:
      frame_hash_total_value  = frame_hash_total_value+frame_hash_data
      # hash_recode.write(frame_hash_data+"\n")
      count_iot_data_number[2]+=1
  frame_hash_total_value_hash = hashlib.sha256((frame_hash_total_value.encode()))
  #print('hash frame_hash_total_value success')
  return sensor_ldr_total_value_hash.hexdigest(), yolo_tag_total_value_hash.hexdigest(), frame_hash_total_value_hash.hexdigest()

def sent_sensor_data_to_DB(sensor_group_number, sync_msg, sensor_id_tmp):
  #choose db collect
  realtime_sensor_data = db.realtime_sensor_data
  #print('send to mongodb')
  create_time = time.strftime("%Y-%m-%d %H:%M:%S")
  if(sync_msg == 1):
    realtime_data_id  = realtime_sensor_data.insert_one({'asset_RFID':sensor_ldr_array[sensor_group_number][0]['asset_RFID'],\
                                                      'camera_id':sensor_camera_array[sensor_group_number][0]['asset_sensor_RFID'], \
                                                      'ldr_id':sensor_ldr_array[sensor_group_number][0]['asset_sensor_RFID'], \
  				                                            'ldr_data':sensor_ldr_array[sensor_group_number][0]['sensor_value'], \
  				                                            'yolo_tag':sensor_camera_array[sensor_group_number][0]['yolo_tag_array'], \
  				                                            'frame_hash':sensor_camera_array[sensor_group_number][0]['frame_hash_array'], \
  				                                            'create_time':create_time,\
                                                      'update_time':create_time})
  elif(sync_msg == 2):
    #camera error
    realtime_data_id  = realtime_sensor_data.insert_one({'asset_RFID':sensor_ldr_array[sensor_group_number][0]['asset_RFID'],\
                                                      'ldr_id':sensor_camera_array[sensor_group_number][0]['asset_sensor_RFID'], \
                                                      'camera_id':sensor_id_tmp, \
  				                                            'ldr_data':sensor_ldr_array[sensor_group_number][0]['sensor_value'], \
  				                                            'yolo_tag':[], \
  				                                            'frame_hash':[], \
  				                                            'create_time':create_time,\
                                                      'update_time':create_time})
  elif(sync_msg == 3):
    #ldr error
    realtime_data_id  = realtime_sensor_data.insert_one({'asset_RFID':sensor_ldr_array[sensor_group_number][0]['asset_RFID'],\
                                                      'ldr_id':sensor_id_tmp, \
                                                      'camera_id':sensor_camera_array[sensor_group_number][0]['asset_sensor_RFID'], \
  				                                            'ldr_data':[], \
  				                                            'yolo_tag':sensor_camera_array[sensor_group_number][0]['yolo_tag_array'], \
  				                                            'frame_hash':sensor_camera_array[sensor_group_number][0]['frame_hash_array'], \
  				                                            'create_time':create_time,\
                                                      'update_time':create_time})
  return realtime_data_id.inserted_id

lock_start_login = 0
lock_end_logout = 0
switch_site_logout = [0,0,0]
def calculator_Sub_run_time_job():
  global switch_site_logout,count_sensor_id_login,lock_start_login,lock_end_logout

  while True:
    end[0] = time.time()
    for login_group_sensor_time_number in range(len(sensor_group_login_time_array)):
      # print('(np.cumsum(sensor_id_login)[len(sensor_id_login)]):'+str((np.cumsum(sensor_id_login)[len(sensor_id_login)])))
      # print('len(sensor_id_array):'+str(len(sensor_id_array)))
      # print('login_group_sensor_time:'+str(login_group_sensor_time))
      # print('int(end[0]-login_group_sensor_time:'+str(int(end[0]-login_group_sensor_time)))
      # print('switch_site_logout[0]:'+str(switch_site_logout[0]))
      #只希望進來一次
      if( ( (lock_start_login == 1) or 
            ((sensor_group_login_time_array[login_group_sensor_time_number] != -1) and (int(end[0]-sensor_group_login_time_array[login_group_sensor_time_number]) == 22))) and
            (lock_start_login != 2)      
      ):
        lock_start_login = 2
        # print('np.cumsum(sensor_id_login):'+str(np.cumsum(sensor_id_login[login_group_sensor_time_number])))
        # print('sensor_id_array:'+str(sensor_id_array[login_group_sensor_time_number]))
        # print('end[0]:'+str(end[0]))
        # print('sensor_group_login_time_array[login_group_sensor_time_number]:'+str(sensor_group_login_time_array[login_group_sensor_time_number]))
        switch_site = db.switch_site
        print("make checksum data")
        login_time = time.strftime("%Y-%m-%d %H:%M:%S")
        checksum_data = ""
        checksum_data = checksum_data+'leager_site'+leager_site_id
        checksum_data = checksum_data+'camera_id_array'+str(sensor_camera_id_array)
        checksum_data = checksum_data+'video_IPFS_hash_array'
        checksum_data = checksum_data+'sensor_ethereum_blockchain_tx_array'
        checksum_data = checksum_data+'ldr_id_array'+str(sensor_ldr_id_array)
        checksum_data = checksum_data+'asset_RFID_array'+str(asset_RFID_list)
        checksum_data = checksum_data+'create_time'+str(login_time)
        checksum_data = checksum_data+'update_time'+str(login_time)
        print(checksum_data)
        checksum =_generate_signature(checksum_data)
        switch_site_id = switch_site.insert_one({ 'leager_site':leager_site_id,\
                                                  'camera_id_array':sensor_camera_id_array, \
                                                  'video_IPFS_hash_array':[], \
                                                  'sensor_ethereum_blockchain_tx_array':[], \
                                                  'ldr_id_array':sensor_ldr_id_array, \
                                                  'asset_RFID_array':asset_RFID_list, \
                                                  'create_time':login_time,\
                                                  'update_time':login_time,\
                                                  'checksum':checksum
        })
        login_logout_recode_array.append(str(switch_site_id.inserted_id))
        trace_asset_in_leagersite = db.trace_asset_in_leagersite

        for asset_trace_asset_in_leagersite_id in asset_trace_asset_in_leagersite_id_array:
          search_data_condition={"_id":ObjectId(asset_trace_asset_in_leagersite_id)}
          search_data_list=trace_asset_in_leagersite.find(search_data_condition)[0]
          find_leager_site_index = 0
          for search_data_index in range(len(search_data_list['leager_site_id_array'])):
            if(search_data_list['leager_site_id_array'][search_data_index] == leager_site_id):
              find_leager_site_index = search_data_index
          search_update_condition={"_id":ObjectId(asset_trace_asset_in_leagersite_id)}
          update_parameter={"$push":{"error."+str(find_leager_site_index):'run'}}
          trace_asset_in_leagersite_id = trace_asset_in_leagersite.update(search_update_condition, update_parameter)
          update_parameter={"$push":{"error_time."+str(find_leager_site_index):login_time}}
          trace_asset_in_leagersite_id = trace_asset_in_leagersite.update(search_update_condition, update_parameter)
          search_data_list=trace_asset_in_leagersite.find(search_data_condition)[0]
          checksum_data = ""
          checksum_data = checksum_data+'asset_RFID'+search_data_list['asset_RFID']
          checksum_data = checksum_data+'leager_site_id_array'+str(search_data_list['leager_site_id_array'])
          checksum_data = checksum_data+'switch_site_id_array'+str(search_data_list['switch_site_id_array'])
          checksum_data = checksum_data+'error'+str(search_data_list['error'])
          checksum_data = checksum_data+'error_item'+str(search_data_list['error_item'])
          checksum_data = checksum_data+'error_time'+str(search_data_list['error_time'])
          checksum_data = checksum_data+'create_time'+str(search_data_list['create_time'])
          checksum_data = checksum_data+'update_time'+str(search_data_list['update_time'])
          print(checksum_data)
          checksum =_generate_signature(checksum_data)
          update_parameter={"$set":{"checksum":checksum}}
          trace_asset_in_leagersite_id = trace_asset_in_leagersite.update(search_update_condition, update_parameter)
        print('test login end2')

      #證明已經有近來過
      if( (((np.cumsum(switch_site_logout)[len(sensor_id_login)-1]) == (len(sensor_id_login))) or
          ((sensor_group_login_time_array[login_group_sensor_time_number] != -1) and  (int(end[0]-sensor_group_login_time_array[login_group_sensor_time_number]) == 3000))) and
          ((len(sensor_ldr_array[login_group_sensor_time_number]) == 0) and (len(sensor_camera_array[login_group_sensor_time_number]) == 0)) and 
          (lock_end_logout == 0)
      ):
        if((np.cumsum(switch_site_logout)[len(sensor_id_login)-1]) == (len(sensor_id_login))):
          print('case 1')
          # print(switch_site_logout)
          # print(sensor_id_login[login_group_sensor_time_number])
          # print(login_group_sensor_time_number)
          # print(sensor_group_login_time_array[login_group_sensor_time_number])

        if(((sensor_group_login_time_array[login_group_sensor_time_number] != -1) and  (int(end[0]-sensor_group_login_time_array[login_group_sensor_time_number]) == 3000))):
          print('case 2')
          print(np.cumsum(switch_site_logout))
        print('send data to switchsite table')
        switch_site = db.switch_site
        #ObjectId("5da98f3ee82b406055c961aa")
        print("make checksum data")
        logout_time = time.strftime("%Y-%m-%d %H:%M:%S")
        checksum_data = ""
        checksum_data = checksum_data+'leager_site'+leager_site_id
        checksum_data = checksum_data+'camera_id_array'+str(sensor_camera_id_array)
        checksum_data = checksum_data+'video_IPFS_hash_array'+str(video_ipfs_hash_array)
        checksum_data = checksum_data+'sensor_ethereum_blockchain_tx_array'+str(sensor_ethereum_blockchain_tx_array)
        checksum_data = checksum_data+'ldr_id_array'+str(sensor_ldr_id_array)
        checksum_data = checksum_data+'asset_RFID_array'+str(asset_RFID_list)
        checksum_data = checksum_data+'create_time'+str(logout_time)
        checksum_data = checksum_data+'update_time'+str(logout_time)
        print(checksum_data)
        checksum =_generate_signature(checksum_data)
        switch_site_id = switch_site.insert_one({'leager_site':leager_site_id,\
                                                 'camera_id_array':sensor_camera_id_array, \
                                                 'video_IPFS_hash_array':video_ipfs_hash_array, \
                                                 'sensor_ethereum_blockchain_tx_array':sensor_ethereum_blockchain_tx_array, \
                                                 'ldr_id_array':sensor_ldr_id_array, \
                                                 'asset_RFID_array':asset_RFID_list, \
                                                 'create_time':logout_time,\
                                                 'update_time':logout_time,\
                                                 'checksum':checksum
        })
        login_logout_recode_array.append(str(switch_site_id.inserted_id))
        trace_asset_in_leagersite = db.trace_asset_in_leagersite
        for asset_trace_asset_in_leagersite_id in asset_trace_asset_in_leagersite_id_array:
          search_data_condition={"_id":ObjectId(asset_trace_asset_in_leagersite_id)}
          search_data_list=trace_asset_in_leagersite.find(search_data_condition)[0]
          find_leager_site_index = 0
          for search_data_index in range(len(search_data_list['leager_site_id_array'])):
            if(search_data_list['leager_site_id_array'][search_data_index] == leager_site_id):
              find_leager_site_index = search_data_index
          search_update_condition={"_id":ObjectId(asset_trace_asset_in_leagersite_id)}
          update_parameter={"$push":{"error."+str(find_leager_site_index):'validation'}}
          trace_asset_in_leagersite_id = trace_asset_in_leagersite.update(search_update_condition, update_parameter)
          update_parameter={"$push":{"error_time."+str(find_leager_site_index):login_time}}
          trace_asset_in_leagersite_id = trace_asset_in_leagersite.update(search_update_condition, update_parameter)
          update_parameter={"$push":{"switch_site_id_array":str(switch_site_id.inserted_id)}}
          trace_asset_in_leagersite_id = trace_asset_in_leagersite.update(search_update_condition, update_parameter)
          search_data_list=trace_asset_in_leagersite.find(search_data_condition)[0]
          checksum_data = ""
          checksum_data = checksum_data+'asset_RFID'+search_data_list['asset_RFID']
          checksum_data = checksum_data+'leager_site_id_array'+str(search_data_list['leager_site_id_array'])
          checksum_data = checksum_data+'switch_site_id_array'+str(search_data_list['switch_site_id_array'])
          checksum_data = checksum_data+'error'+str(search_data_list['error'])
          checksum_data = checksum_data+'error_item'+str(search_data_list['error_item'])
          checksum_data = checksum_data+'error_time'+str(search_data_list['error_time'])
          checksum_data = checksum_data+'create_time'+str(search_data_list['create_time'])
          checksum_data = checksum_data+'update_time'+str(search_data_list['update_time'])
          print(checksum_data)
          checksum =_generate_signature(checksum_data)
          update_parameter={"$set":{"checksum":checksum}}
          trace_asset_in_leagersite_id = trace_asset_in_leagersite.update(search_update_condition, update_parameter)
        print("make checksum data")
        checksum_data = ""
        checksum_data = checksum_data+'leager_site'+leager_site_id
        checksum_data = checksum_data+'error_pos'+str(find_leager_site_index)
        checksum_data = checksum_data+'switch_site_login_id'+str(login_logout_recode_array[0])
        checksum_data = checksum_data+'switch_site_logout_id'+str(login_logout_recode_array[1])
        checksum_data = checksum_data+'create_time'+str(logout_time)
        checksum_data = checksum_data+'update_time'+str(logout_time)
        # print(checksum_data)
        checksum =_generate_signature(checksum_data)
        background_trace_back_job = db.background_trace_back_job
        background_trace_back_job = background_trace_back_job.insert_one({'leager_site':leager_site_id,\
                                                                          'error_pos':find_leager_site_index, \
                                                                          'switch_site_login_id':login_logout_recode_array[0], \
                                                                          'switch_site_logout_id':login_logout_recode_array[1], \
                                                                          'create_time':logout_time,\
                                                                          'update_time':logout_time,\
                                                                          'checksum':checksum
        })

        sensor_group_table = db.sensor_group
        checksum_data = ""
        checksum_data = checksum_data+'switch_site_id'+str(switch_site_id.inserted_id)
        checksum_data = checksum_data+'asset_RFID_list'+str(asset_RFID_list)
        checksum_data = checksum_data+'sensor_ldr_group'+str(sensor_ldr_group)
        checksum_data = checksum_data+'sensor_camera_group'+str(sensor_camera_group)
        checksum_data = checksum_data+'create_time'+str(login_time)
        checksum_data = checksum_data+'update_time'+str(login_time)
        print(checksum_data)
        checksum =_generate_signature(checksum_data)
        sensor_group_table_id = sensor_group_table.insert_one({ 'leager_site':leager_site_id,\
                                                  'asset_RFID_list':asset_RFID_list, \
                                                  'sensor_ldr_group':sensor_ldr_group, \
                                                  'sensor_camera_group':sensor_camera_group, \
                                                  'create_time':login_time, \
                                                  'update_time':login_time, \
                                                  'checksum':checksum
        })
        print('stroe data in switchsite table')
        print('leave switchstie')
        print('leave switchstie2')
        lock_end_logout = 1

mqtt_client.on_connect = on_BrokerConnect
mqtt_client.on_message = on_BrokerMessage

#client.connect("soldier.cloudmqtt.com",10129, 60)
#client.connect("iot.eclipse.org", 1883,60)
#client.connect("mqtt.eclipse.org", 1883,60)
mqtt_client.username_pw_set('aucsie07','1234')
mqtt_client.connect_async("192.168.1.133", 1883,60)
#client.connect("192.168.0.231", 8883,60)

calculator_Sub_run_time_Thread = threading.Thread(target = calculator_Sub_run_time_job)
calculator_Sub_run_time_Thread.start()

process_raw_data_Thread = threading.Thread(target = process_raw_data)
process_raw_data_Thread.start()

mqtt_client.loop_forever()
