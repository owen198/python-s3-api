import boto
import boto.s3.connection
from boto.s3.key import Key

import pandas as pd
import pandas.io.sql as sqlio

import numpy as np
import os
import json
import datetime
import binascii
import struct
import requests
import psycopg2

from flask import Flask
from flask import request
from flask import jsonify
from nptdms import TdmsFile
from pymongo import MongoClient
# from influxdb import DataFrameClient

app = Flask(__name__)

@app.route("/", methods=['GET','POST'])
def test_1():
    """
    for simple json.
    official page: should return 200 ok. Used for "Test connection" on the datasource config page.
    """
    str_msg = 'test simple json with /'

    return jsonify({'msg': str_msg}), 200

#@app.route('/blob/api/v1.0/get_content', methods=['POST'])
@app.route('/query', methods=['POST'])
def get_content():
    
    SAMPLE_RATE = 8192
    DISPLAY_POINT = 65536   

    # retrieve post JSON object
    jsonobj = request.get_json(silent=True)
    print(jsonobj)
    target_obj = jsonobj['targets'][0]['target']
    date_obj = jsonobj['range']['from']
    DATE = datetime.datetime.strptime(date_obj, '%Y-%m-%dT%H:%M:%S.%fZ')
    DATE = DATE + datetime.timedelta(hours=8)
    #DATE = DATE + datetime.timedelta(hours=8) - datetime.timedelta(days=1)    
    DATE = DATE.strftime('%Y-%m-%d')

    EQU_ID = target_obj.split('@')[0]
    FEATURE = target_obj.split('@')[1]
    TYPE = target_obj.split('@')[2]
    SignalType = target_obj.split('@')[-1]
    SPECIFIC_TIME = target_obj.split('@')[-2]
        
    print('EQU_ID=' + EQU_ID)
    print('Feature=' + FEATURE)
    print('Type=' + TYPE)
    print('SignalType=' + SignalType)
    print('Query Date=' + DATE)

    
    # Dr. Ho: we use specified time instead of search range
    # User specified a timestamp
    SPECIFIC_TIME = SPECIFIC_TIME.split(r'\.')[0]

    # check bin or tdms
    DEVICE_NAME = query_device_name (EQU_ID)
    if '_vpod' in DEVICE_NAME:
        #bin
        print('user specified time and date')
        TS = datetime.datetime.fromtimestamp(int(SPECIFIC_TIME[0:10]))
        TS = TS + datetime.timedelta(hours=8)
    else:
        #tdms
        if SPECIFIC_TIME.isdigit() and len(SPECIFIC_TIME) > 3:
            print('user specified time and date')
            TS = datetime.datetime.fromtimestamp(int(SPECIFIC_TIME[0:10]))
            TS = TS + datetime.timedelta(hours=8)
        else:
            print('user specified by query')
            TS = query_timestamp (TYPE, FEATURE, EQU_ID, DATE)


    print('Feature assorcated timestamp in Query Date=', TS)

    # establish connection to s3 and search bin file that the mostest close to query date
    S3_BUCKET = get_s3_bucket()


    # TODO: use a condition(if) here if merge bin / tdms
    if '_vpod' in DEVICE_NAME:
        PATH_DEST = '#1HSM/ROT/vPodPRO/' + DEVICE_NAME + '/' + str(TS.strftime("%Y")) + '/' + str(TS.strftime("%m")) + '/' + str(TS.strftime("%d")) + '/'
    else:
        PATH_DEST = '#1HSM/ROT/TDMS/' + DEVICE_NAME + '/' + str(TS.strftime("%Y")) + '/' + str(TS.strftime("%m")) + '/' + str(TS.strftime("%d")) + '/'
    print("PATH_DEST:",PATH_DEST)
    PATH_DEST=PATH_DEST.encode('utf-8').strip()

    HOUR = str(TS.strftime("%H"))
    MIN = str(TS.strftime("%M"))
    SECOND = str(TS.strftime("%S"))
    
    # TODO: use a condition(if) here if merge bin / tdms
    if '_vpod' in DEVICE_NAME:
        FILE_NAME = 'Raw Data-' + DEVICE_NAME + '-' + HOUR + '-'+ MIN + '-' + SECOND + '_25600.bin'
    else:
        FILE_NAME = 'Raw Data-' + DEVICE_NAME + '-'+ HOUR + '-'+ MIN + '-' + SECOND + '.tdms'
    print("FILE_NAME:",FILE_NAME)
    FILE_NAME=FILE_NAME.encode('utf-8').strip()
    

    # goto bucket and get file accroding to the file name
    s3_data = os.path.join(PATH_DEST, FILE_NAME)
    key = S3_BUCKET.get_key(s3_data)

    try:
        key.get_contents_to_filename(FILE_NAME)
    except:
        print('File not found')
        return 'File not found'

    #'#1HSM/ROT/vPodPRO/#1內冷式ROT Roller WS_vpod/2020/08/01/'
    #'#1HSM/ROT/vPodPRO/#1內冷式ROT Roller WS_vpod/2020/08/01/'

    #'Raw Data-#1內冷式ROT Roller WS_vpod-00-56-28_25600.bin'
    #'Raw Data-#1內冷式ROT Roller WS_vpod-00-56-26_25600.bin'

    if '_vpod' in DEVICE_NAME:
        DATA_DF, DATA_LENGTH = convert_bin (FILE_NAME, DISPLAY_POINT)
    else:
        DATA_DF, DATA_LENGTH = convert_tdms (FILE_NAME, DISPLAY_POINT)

    print ('data_lenght=', DATA_LENGTH)

    if SignalType=='velocity':
        print('velocity change, size from:')
        print(DATA_DF.shape)
        DATA_DF = pd.DataFrame(get_velocity_mms_from_acceleration_g(DATA_DF.values.T,1.0/8192)).T
        print('velocity change, size to:')
        print(DATA_DF.shape)
        print(DATA_DF.values)
        print(type(DATA_DF.values))

    # calculate start-time and end-time for grafana representation
    S3_BUCKET = get_s3_bucket()

    TIME_START = TS.strftime('%Y-%m-%d') + 'T' + HOUR + ':' + MIN + ':' + SECOND
    TIME_START = datetime.datetime.strptime(TIME_START, '%Y-%m-%dT%H:%M:%S')
    TIME_START = TIME_START - datetime.timedelta(hours=8)
    TIME_START = TIME_START.timestamp() * 1000
    TIME_DELTA = float(float(DATA_LENGTH / SAMPLE_RATE) / DISPLAY_POINT) * 1000
    print ('Grafana x-axis TIME_START=', TIME_START)
    print('Datatime='+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # delete file which stored in local
    os.remove(FILE_NAME)

    # combine response json object follow the rule of grafana simpleJSON
    RETURN = combine_return (TIME_START, TIME_DELTA, DATA_DF, DATA_LENGTH)

    return RETURN

def convert_tdms (filename, DISPLAYPOINT):
    bytes_read = TdmsFile(filename)
    
    tdms_groups = bytes_read.groups()
    # print(tdms_groups)
    tdms_groups=str(tdms_groups)
    tdms_Variables_1 = bytes_read.group_channels(tdms_groups.split("'")[1])
    # print(tdms_Variables_1)
    tdms_Variables_1=str(tdms_Variables_1)
    MessageData_channel_1 = bytes_read.object((tdms_Variables_1.split("'")[1]),tdms_Variables_1.split("'")[3])
    # print(MessageData_channel_1)
    MessageData_data_1 = MessageData_channel_1.data
    # MessageData_data_1 
  
    return_df  = pd.DataFrame(MessageData_data_1)

    print('len3',len(return_df))
    return_df = return_df.T
    print('len4',len(return_df))

    file_length = len(return_df.columns) 

    #length = file_length / DISPLAYPOINT

    return return_df, file_length

def convert_bin (filename, DISPLAYPOINT):
    bytes_read = open(filename, "rb").read()
    size = [hexint(bytes_read[(i*4):((i+1)*4)]) for i in range(2)]
    signal = [struct.unpack('f',bytes_read[(i*4):((i+1)*4)]) for i in range(2,2+size[0]*size[1])]
    data = np.array(signal).reshape(size)

    return_df = pd.DataFrame(data = data)
    return_df = return_df.T
    file_length = len(return_df)

    #length = file_length / DISPLAYPOINT

    return return_df, file_length

def hexint(b,bReverse=True): 
    return int(binascii.hexlify(b[::-1]), 16) if bReverse else int(binascii.hexlify(b), 16)

def query_device_name (EQU_ID):
    PG_IP = "192.168.123.238"
    PG_USER = "6e6b8fc2-ea3d-412e-9806-692a4aea5c0e"
    PG_PASS = "huu1enrd9rrsptr86kvapqk4pe"
    PG_DB = "214c2b8f-c62b-4133-82a8-93eb2dc59277"

    conn = psycopg2.connect(
                            host = PG_IP,
                            database = PG_DB,
                            user = PG_USER,
                            password = PG_PASS)

    sql = r'SELECT * FROM "CSC_FOMOS"."Y4_Channel_List";'
    tag_list_pd = sqlio.read_sql_query(sql, conn)
    conn.close()

    device_name = tag_list_pd[tag_list_pd['channelID'] == EQU_ID]['channel_name'].values[0]

    return device_name

def get_s3_bucket ():
    # load value of key for access blob container (bucket)
    ACCESS_KEY = 't7user1'
    SECRET_KEY = 'Bhw0MdOHfFL9h03u3epl4AoLxl/sOu0UvELUBL1h'
    HOST = 'object.csc.com.tw'
    PORT = 9020
#     BUCKET_NAME = 'fomos-w4'
    BUCKET_NAME = 'Y4_HSM'    
    # establish connection between blob storage and this client app
    s3_connection = boto.connect_s3(
                   aws_access_key_id = ACCESS_KEY,
                    aws_secret_access_key = SECRET_KEY,
                   host = HOST,
                   port = PORT,
                   is_secure=False,               # uncomment if you are not using ssl
                   calling_format = boto.s3.connection.OrdinaryCallingFormat(),
                 )
    bucket = s3_connection.get_bucket(BUCKET_NAME, validate=False)

    return bucket
    
def combine_return (TIME_START, TIME_DELTA, BIN_DF, BIN_LENGTH):
    
    # load 'data' and 'index' in bin file, and append it into a list
    # follow data format from Grafana: https://github.com/grafana/simple-json-datasource/blob/master/README.md
    jsonobj_mean = json.loads(BIN_DF.to_json(orient='split'))

    datapoints_array_mean = []

    print('len1', BIN_LENGTH)
    print('len2', len(jsonobj_mean))

    for i in range(0, BIN_LENGTH):
        datapoints_array_mean.append([jsonobj_mean['data'][i][0], TIME_START])
        TIME_START = float(TIME_START) + TIME_DELTA

    # construct json array for API response
    dict_data_mean = {}
    dict_data_mean["target"] = 'original'
    dict_data_mean["datapoints"] = datapoints_array_mean
    
    jsonarr = json.dumps([dict_data_mean])
    
    
    return str(jsonarr)

def query_timestamp (TYPE, feature, EQU_ID, time_start):
    
    ## MongoDB Configuration

    key = "da8ab85c90acca0045835b88c9c048j5"
    url = "https://api-dccs.fomos.csc.com.tw/v1/serviceCredentials/" + key
    # url = "https://api-dccs.fomos.csc.com.tw/v1/serviceCredentials/da8ab85c90acca0045835b88c9c048j5"

    headers = {
        'Accept': "*/*",
        'Cache-Control': "no-cache",
        'Host': "api-dccs.fomos.csc.com.tw"
    }
    response = requests.request("GET", url, headers=headers, verify=False)

    Json_array = json.loads(response.text)

    mgdb_host = Json_array['credential']['host']
    mgdb_port = Json_array['credential']['port']
    mgdb_database = Json_array['credential']['database']
    mgdb_username = Json_array['credential']['username']
    mgdb_password = Json_array['credential']['password']
    mgdb_collection = 'Y4_features'

    time_start = time_start.replace("/", "-")
    time_end = datetime.datetime.strptime(time_start, '%Y-%m-%d') + datetime.timedelta(days=1)
#     print('time_end', type(time_end), time_end)
    time_end = time_end.strftime("%Y-%m-%d")
    print("time_start",time_start)
    print('time_end', type(time_end), time_end)
    ## Query MongoDB
    measurement, data = read_MongoDB_data(EQU_ID,
                                            time_start = time_start,
                                            time_end = time_end)


    if TYPE == 'max':
        max_value = data.sort_values(by=[feature])[feature].iloc[-1]
    elif TYPE == 'median':
        max_value = data.sort_values(by=[feature])[feature][len(data)//2]
    else:
        max_value = data.sort_values(by=[feature])[feature].iloc[0]

    print('value=', max_value)
    ## Retrive timestamp
    index_series = data[feature]
    dt64 = index_series[index_series == max_value].index.values[0]
    TS = datetime.datetime.utcfromtimestamp(dt64.tolist()/1e9) 
    print('TS=',TS)
    return TS
    
    
def butter_highpass(cutoff, fs, order=5):
    from scipy import signal
    nyq = 0.5 * fs
    normal_cutoff = cutoff / nyq
    b, a = signal.butter(order, normal_cutoff, btype='high', analog=False)
    return b, a

def butter_highpass_filter(data, cutoff, fs, order=5):
    from scipy import signal
    b, a = butter_highpass(cutoff, fs, order=order)
    y = signal.filtfilt(b, a, data, method="gust")
    return y    

def get_velocity_mms_from_acceleration_g(data, TS):
    from scipy.integrate import cumtrapz

    # acceleration = (data - data.mean()) * 9806
    # velocity = TS * cumtrapz(acceleration, initial=0.)
    # data = butter_highpass_filter(data, cutoff=5, fs=1/TS)
    data = butter_highpass_filter(data, cutoff=5, fs=1/TS, order=3)
    velocity = cumtrapz(data, dx=TS, initial=0)
    velocity = (velocity - velocity.mean()) * 9806
    print('TS=',TS)
    print('DATA=',data)
    print(type(data))
    # print('Acc=',acceleration)
    print('Vel=',velocity)
    return velocity


def read_MongoDB_data(EQU_ID,
                       time_start='', 
                       time_end=''):


    mgdb_host = '10.100.10.1'
    mgdb_port = '27017'
    mgdb_db = 'd21d5987-3b65-4fae-9b02-79f72b39b735'
    mgdb_user = '0c1e58e3-8643-4ff3-8633-7820f10f4902'
    mgdb_pass= 'SPRXaEL2RIIeve2lsHV9oAjCc'
    mgdb_collection = 'y4_features'
    
    client = MongoClient('mongodb://%s:%s@%s/%s' % (mgdb_user, mgdb_pass, mgdb_host, mgdb_db))

    db = client[mgdb_db]
    collection = db[mgdb_collection]

    measurement = db.list_collection_names()

    gt = int(datetime.datetime.strptime(time_start + ' 0:0:0' , '%Y-%m-%d %H:%M:%S').strftime('%s'))
    lt = int(datetime.datetime.strptime(time_end + ' 23:59:59', '%Y-%m-%d %H:%M:%S').strftime('%s'))

    data = pd.DataFrame(list(collection.find({
        "$and":[ 
            { 'timestamp': {'$gt':gt, '$lt': lt} }, 
            { 'device': EQU_ID } ] })))
            
    print('data length in MongoDB', len(data))
    data.index = (pd.to_datetime(data['timestamp'], unit='s'))
    
    return measurement, data

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
