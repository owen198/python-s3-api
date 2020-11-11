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



@app.route('/query', methods=['POST'])
def get_content():
    
    display_points = 65536


    # retrieve post JSON object
    jsonobj = request.get_json(silent=True)
    print(jsonobj)
    target_obj = jsonobj['targets'][0]['target']
    date_obj = jsonobj['range']['from']
    DATE = datetime.datetime.strptime(date_obj, '%Y-%m-%dT%H:%M:%S.%fZ')
    DATE = DATE + datetime.timedelta(hours=8)
    #DATE = DATE + datetime.timedelta(hours=8) - datetime.timedelta(days=1)    
    DATE = DATE.strftime('%Y-%m-%d')

    device_id = target_obj.split('@')[0]
    FEATURE = target_obj.split('@')[1]
    TYPE = target_obj.split('@')[2]
    SignalType = target_obj.split('@')[-1]
    precision_time = target_obj.split('@')[-2]
        
    print('device_id=' + device_id)
    print('Feature=' + FEATURE)
    print('Type=' + TYPE)
    print('SignalType=' + SignalType)
    print('Query Date=' + DATE)

    

    # consider timestamp
    precision_time = precision_time.split(r'\.')[0]
    device_name = query_device_name (device_id)
    if '_vpod' in device_name:
        #bin
        print('user specified time and date')
        TS = datetime.datetime.fromtimestamp(int(precision_time[0:10]))
        TS = TS + datetime.timedelta(hours=8)
    else:
        #tdms
        if precision_time.isdigit() and len(precision_time) > 3:
            print('user specified time and date')
            TS = datetime.datetime.fromtimestamp(int(precision_time[0:10]))
            TS = TS + datetime.timedelta(hours=8)
        else:
            print('user specified by query')
            TS = query_timestamp (TYPE, FEATURE, device_id, DATE)

    print('Feature assorcated timestamp in Query Date=', TS)



    # Define s3 prefix and filename based on timestamp and id
    year = str(TS.strftime("%Y"))
    month = str(TS.strftime("%m"))
    day = str(TS.strftime("%d"))
    hour = str(TS.strftime("%H"))
    minutes = str(TS.strftime("%M"))
    second = str(TS.strftime("%S"))

    if '_vpod' in device_name:
        prefix = '#1HSM/ROT/vPodPRO/' + device_name + '/' + year + '/' + month + '/' + day + '/'
        filename = 'Raw Data-' + device_name + '-' + hour + '-'+ minutes + '-' + second + '_25600.bin'
    else:
        prefix = '#1HSM/ROT/TDMS/' + device_name + '/' + year + '/' + month + '/' + day + '/'
        filename = 'Raw Data-' + device_name + '-'+ hour + '-'+ minutes + '-' + second + '.tdms'

    print("prefix:", prefix)
    print("filename:", filename)
    
    filename_fs = filename

    # connect to bucket and get file
    prefix = prefix.encode('utf-8').strip()
    filename = filename.encode('utf-8').strip()
    s3_data = os.path.join(prefix, filename)
    #S3_BUCKET = get_s3_bucket()
    key = get_s3_bucket().get_key(s3_data)

    try:
        key.get_contents_to_filename(filename)
    except:
        print('File not found')
        return 'File not found'


    # Define sampling rate
    if '_vpod' in device_name:
        sampling_rate =  int(filename_fs.split('_')[-1].split('.')[0])
    else:
        ti = read_lv2_names(filename_fs)[0]
        properties = read_lv2_properties(filename_fs,ti)
        dt = properties[u'wf_increment']
        sampling_rate = 1 / dt
    print('sampling_rate:', sampling_rate)


    # decompress tdms/bin file, load as pandas dataframe
    if '_vpod' in device_name:
        data_df, data_len = convert_bin (filename)
    else:
        data_df, data_len = convert_tdms (filename)

    # add by Dr. Ho
    if SignalType=='velocity':
        #print('velocity change, size from:')
        #print(data_df.shape)
        data_df = pd.DataFrame(get_velocity_mms_from_acceleration_g(data_df.values.T,1.0/sampling_rate)).T
        #print('velocity change, size to:')
        #print(data_df.shape)
        #print(data_df.values)
        #print(type(data_df.values))

    print('data_df.head(5)', data_df.head(5))
    print('length', len(data_df))

    # calculate start-time and end-time for grafana representation
    time_start = TS.strftime('%Y-%m-%d') + 'T' + hour + ':' + minutes + ':' + second
    time_start = datetime.datetime.strptime(time_start, '%Y-%m-%dT%H:%M:%S')
    time_start = time_start - datetime.timedelta(hours=8)
    time_start = time_start.timestamp() * 1000
    #time_delta = float(float(data_len / sampling_rate) / display_points) * 1000
    time_delta = float(1 / sampling_rate) * 1000

    print ('Grafana x-axis time_start=', time_start)
    print ('Grafana x-axis time_delta=', time_delta)


    # delete file which stored in local
    os.remove(filename)

    # combine response json object follow the rule of grafana simpleJSON
    return combine_return (time_start, time_delta, data_df, data_len)



def read_lv2_properties(filename, lv2_name):    
    tdms_file = TdmsFile(filename)
    #tdms_file = TdmsFile(filename)
    objNames = list(tdms_file.objects.keys())
    #wh_lv3 = find([len(obj.split('/'))==3 for obj in objNames[:]])
    wh_lv3 = np.argwhere([len(obj.split('/'))==3 for obj in objNames[:]])[0]
    lv3s = [objNames[i] for i in wh_lv3]
    lv2_names = [lv.split('/')[1] for lv in lv3s]
    #wh = find([n==lv2_name for n in lv2_names])[0]
    wh = np.argwhere([n==lv2_name for n in lv2_names])[0][0]
    wh = wh_lv3[wh]
    ret = tdms_file.objects[objNames[wh]].properties    
    return ret



def read_lv2_names(filename):    
    tdms_file = TdmsFile(filename)
    objNames = list(tdms_file.objects.keys())
    lv2 = list(set([obj.split('/')[1] for obj in objNames[1:]]))
    return lv2


def convert_tdms (filename):
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

    #return_df = return_df.T
    #print('len4',len(return_df))

    file_length = len(return_df) 

    #length = file_length / DISPLAYPOINT

    return return_df, file_length

def convert_bin (filename):
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

    for i in range(0, BIN_LENGTH):
        datapoints_array_mean.append([jsonobj_mean['data'][i][0], TIME_START])
        TIME_START = float(TIME_START) + TIME_DELTA

    # construct json array for API response
    dict_data_mean = {}
    dict_data_mean["target"] = 'original'
    dict_data_mean["datapoints"] = datapoints_array_mean
    
    jsonarr = json.dumps([dict_data_mean])
    
    
    return str(jsonarr)

def query_timestamp (TYPE, feature, device_id, time_start):
    
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
    time_end = time_end.strftime("%Y-%m-%d")

    ## Query MongoDB
    measurement, data = read_MongoDB_data (device_id,
                                            time_start = time_start,
                                            time_end = time_end)


    if TYPE == 'max':
        max_value = data.sort_values(by=[feature])[feature].iloc[-1]
    elif TYPE == 'median':
        max_value = data.sort_values(by=[feature])[feature][len(data)//2]
    else:
        max_value = data.sort_values(by=[feature])[feature].iloc[0]

    #print('value=', max_value)
    ## Retrive timestamp
    index_series = data[feature]
    dt64 = index_series[index_series == max_value].index.values[0]
    TS = datetime.datetime.utcfromtimestamp(dt64.tolist()/1e9) 
    #print('TS=',TS)
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
    #print('TS=',TS)
    #print('DATA=',data)
    #print(type(data))
    # print('Acc=',acceleration)
    #print('Vel=',velocity)
    return velocity


def read_MongoDB_data(device_id,
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
            { 'device': device_id } ] })))
            
    print('data length in MongoDB', len(data))
    data.index = (pd.to_datetime(data['timestamp'], unit='s'))
    
    return measurement, data

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
