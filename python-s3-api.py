import boto
import boto.s3.connection
from boto.s3.key import Key

import pandas as pd
import numpy as np
import os
import json
import datetime

from nptdms import TdmsFile
# import binascii

import struct

import requests

from pymongo import MongoClient

# import scipy

from flask import Flask
from flask import request
from flask import jsonify
# from influxdb import DataFrameClient

app = Flask(__name__)


@app.route("/search", methods=['GET','POST'])
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
#     DISPLAY_POINT = 65536
    DISPLAY_POINT = 51180    
#     retrieve post JSON object
    jsonobj = request.get_json(silent=True)
    print(jsonobj)
    target_obj = jsonobj['targets'][0]['target']
    date_obj = jsonobj['range']['from']
    DATE = datetime.datetime.strptime(date_obj, '%Y-%m-%dT%H:%M:%S.%fZ')
    DATE = DATE + datetime.timedelta(hours=8)
#     DATE = DATE + datetime.timedelta(hours=8) - datetime.timedelta(days=1)    
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
    
    
    # User specified a timestamp
    SPECIFIC_TIME = SPECIFIC_TIME.split(r'\.')[0]
    if SPECIFIC_TIME.isdigit() and len(SPECIFIC_TIME) > 3:
        print('user specified time and date')
        TS = datetime.datetime.fromtimestamp(int(SPECIFIC_TIME[0:10]))
        TS = TS + datetime.timedelta(hours=8)
        print('Feature assorcated timestamp in Query Date=', TS)
    else:
        print('user specified by query')
        TS = query_timestamp (TYPE, FEATURE, EQU_ID,DATE)
        print('Feature assorcated timestamp in Query Date=', TS)

    # establish connection to s3 and search bin file that the mostest close to query date
    S3_BUCKET = get_s3_bucket()
    
    # parsing EQU_ID to get SMB_ID, for combining S3 Path
    
    MACHINE_ID = query_smb_byDigit(EQU_ID)
    PATH_DEST = MACHINE_ID  + str(TS.strftime("%Y")) + '/' + str(TS.strftime("%m")) + '/' + str(TS.strftime("%d")) + '/'
    print("PATH_DEST:",PATH_DEST)
    PATH_DEST=PATH_DEST.encode('utf-8').strip()

    FILE_NAME = query_file (TS, S3_BUCKET, PATH_DEST,EQU_ID)
    print("FILE_NAME:",FILE_NAME)
    FILE_NAME=FILE_NAME.encode('utf-8').strip()
    print("FILE_NAME:",FILE_NAME)    
    
    # to catch bin file not exist issue, for example: 1Y510110107, 2019-05-21T20:49:55.000Z
    if FILE_NAME is 'null':
        return 'File not found'

    # goto bucket and get file accroding to the file name
    s3_tdms_data = os.path.join(PATH_DEST, FILE_NAME)
#     print("s3_tdms_data: ",s3_tdms_data)
    key = S3_BUCKET.get_key(s3_tdms_data)
#     print('key: ', key)
#     print('tdms file that the most closest to timestamp in Query Date=)
    
    # download content for convert bin to plantext
    try:
        key.get_contents_to_filename(FILE_NAME)
    except:
        print('File not found')
        return 'File not found'

    tdms_DF,tdms_LENGTH = convert_tdms(FILE_NAME, DISPLAY_POINT)
    if SignalType=='velocity':
        print('velocity change, size from:')
        print(tdms_DF.shape)
        tdms_DF = pd.DataFrame(get_velocity_mms_from_acceleration_g(tdms_DF.values.T,1.0/8192)).T
        print('velocity change, size to:')
        print(tdms_DF.shape)
        print(tdms_DF.values)
        print(type(tdms_DF.values))
    # insert_to_influxdb(tdms_DF)

    # calculate start-time and end-time for grafana representation
    S3_BUCKET = get_s3_bucket()
    filename = 'tag_list_2.csv'
    tag_list = os.path.join('/', filename)
    key =S3_BUCKET.get_key(tag_list)
    key.get_contents_to_filename(filename)
    df = pd.read_csv('tag_list_2.csv', encoding='big5')
    df.columns = ['Channel_Name','ID Number','產線','Station','','Device','','Channel_Number']
    df1 = df.loc[ df['ID Number'] == EQU_ID ]
    df1 = df1.values.tolist()
    Channel_Name = df1[0][0]
    station = df1[0][3]
    os.remove(filename)
    
    if station == '1FM':
        HOUR = FILE_NAME.decode().split('-')[2]
        MIN = FILE_NAME.decode().split('-')[3]
        SECOND = FILE_NAME.decode().split('-')[4].split('.')[0]
    elif station =='2FM':
        HOUR = FILE_NAME.decode().split('-')[2]
        MIN = FILE_NAME.decode().split('-')[3]
        SECOND = FILE_NAME.decode().split('-')[4].split('.')[0]
    else:    
        HOUR = FILE_NAME.decode().split('-')[3]
        MIN = FILE_NAME.decode().split('-')[4]
        SECOND = FILE_NAME.decode().split('-')[5].split('.')[0]


    TIME_START = TS.strftime('%Y-%m-%d') + 'T' + HOUR + ':' + MIN + ':' + SECOND
    TIME_START = datetime.datetime.strptime(TIME_START, '%Y-%m-%dT%H:%M:%S')
    TIME_START = TIME_START - datetime.timedelta(hours=8)
    TIME_START = TIME_START.timestamp() * 1000
    TIME_DELTA = float(float(tdms_LENGTH / SAMPLE_RATE) / DISPLAY_POINT) * 1000
    print ('Grafana x-axis TIME_START=', TIME_START)
    print('Datatime='+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # delete file which stored in local
    os.remove(FILE_NAME)

    # combine response json object follow the rule of grafana simpleJSON
    RETURN = combine_return (TIME_START, TIME_DELTA, tdms_DF,tdms_LENGTH)

    return RETURN

def query_file (TS, bucket, PATH_DEST,EQU_ID):
    S3_BUCKET = get_s3_bucket()
    filename = 'tag_list_2.csv'
    tag_list = os.path.join('/', filename)
    key =S3_BUCKET.get_key(tag_list)
    key.get_contents_to_filename(filename)
    df = pd.read_csv('tag_list_2.csv', encoding='big5')
    df.columns = ['Channel_Name','ID Number','產線','Station','','Device','','Channel_Number']
    df1 = df.loc[ df['ID Number'] == EQU_ID ]
    df1 = df1.values.tolist()
    Channel_Name = df1[0][0]
    station = df1[0][3]
    #time 
    os.remove(filename)
    TS_H = TS.strftime('%H')
    TS_M = TS.strftime('%M')
    TS_S = TS.strftime('%S')

    if station == '505':
        filename = 'Raw Data-'+ Channel_Name +'-rolling-'+ TS_H + "-"+ TS_M + "-"+ TS_S + ".tdms"
    elif station == '506':
        filename = 'Raw Data-'+ Channel_Name +'-rolling-'+ TS_H + "-"+ TS_M + "-"+ TS_S + ".tdms"
    elif station == '307':
        filename = 'Raw Data-'+ Channel_Name +'-rolling-'+ TS_H + "-"+ TS_M + "-"+ TS_S + ".tdms"
    elif station == '308':
        filename = 'Raw Data-'+ Channel_Name +'-rolling-'+ TS_H + "-"+ TS_M + "-"+ TS_S + ".tdms"
    elif station == '1FM':
        filename = 'Raw Data-'+ Channel_Name +'-'+ TS_H + "-"+ TS_M + "-"+ TS_S + ".tdms"
    elif station == '2FM':
        filename = 'Raw Data-'+ Channel_Name +'-'+ TS_H + "-"+ TS_M + "-"+ TS_S + ".tdms"
    else:
        print("query_file error")

    # filename = 'Raw Data-'+ Channel_Name +'-rolling-'+ TS_H + "-"+ TS_M + "-"+ TS_S + ".tdms"
#     filename = f"Raw Data-{Channel_Name}-rolling-{TS_H}-{TS_M}-{TS_S}.tdms"

    return filename

def get_s3_bucket ():
    # load value of key for access blob container (bucket)
    #ACCESS_KEY = 'cc0b4b06affd4f599dff7607f1556811'
    #SECRET_KEY = 'U7fxYmr8idml083N8zo7JRddXiNbyCmNN'
    #HOST = '192.168.123.226'
    #PORT = 8080
    #BUCKET_NAME = 'FOMOS-W4'    
    
    # switch to F3_S3
    ACCESS_KEY = 't7user1'
    SECRET_KEY = 'Bhw0MdOHfFL9h03u3epl4AoLxl/sOu0UvELUBL1h'
    HOST = 'object.csc.com.tw'
    PORT = 9020
    BUCKET_NAME = 'W4_FOMOS' 
    
    
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

def query_smb_byDigit (EQU_ID):
    S3_BUCKET = get_s3_bucket()
    filename = 'tag_list_2.csv'
    tag_list = os.path.join('/', filename)
    key =S3_BUCKET.get_key(tag_list)
    key.get_contents_to_filename(filename)

    df = pd.read_csv('tag_list_2.csv', encoding='big5')

    df.columns = ['Channel_Name','ID Number','產線','Station','','Device','','Channel_Number']
    df['line'] = 0
    for idx, row in df.iterrows():  
        Station = row['Station']
        if Station == '505':
            line = ("條二")
        elif Station == '506':
            line = ("條二")

        elif Station == '307':
            line = ("線一")        
        elif Station == '308':
            line = ("線一")        
        elif Station == '310':
            line = ("線一")
        elif Station == '314':
            line = ("線一")        
        elif Station == '315':
            line = ("線一")    

        elif Station == '1FM':
            line = ("線二")        
        elif Station == '1RSM':
            line = ("線二")        
        elif Station == '2FM':
            line = ("線二")        
        elif Station == '2RSM':
            line = ("線二")                        
        else:
            line = "NaN"
        df.loc[idx, 'line'] = line

    df1 = df.loc[ df['ID Number'] == EQU_ID ]
    df1 = df1.values.tolist()

    Channel_Name = df1[0][0]
    Station = df1[0][3]
    line = df1[0][8]  
    
    MACHINE_ID = line + '/' + Station + '/' + Channel_Name + '/' 
    
#     MACHINE_ID = (MACHINE_ID).encode('utf-8').strip()
    os.remove(filename)
    return MACHINE_ID

def query_smb (bucket, EQU_ID):
    
    # load folder and sub folder from bucket into a dataframe
    smb_df = pd.DataFrame(columns=['smb_number', 'EQU_ID', 'n'])
    for folder in bucket.list(delimiter='/'):
        for subfolder in bucket.list(delimiter='/', prefix=folder.name):
            smb_df = smb_df.append(pd.Series(subfolder.name.split('/'), 
                                             index=['smb_number', 'EQU_ID', 'n']), ignore_index=True)

    machine_id = smb_df[smb_df['EQU_ID']==EQU_ID]['smb_number'].values[0]
    return machine_id

def combine_return (TIME_START, TIME_DELTA, tdms_DF, tdms_LENGTH):
    
    # load 'data' and 'index' in bin file, and append it into a list
    # follow data format from Grafana: https://github.com/grafana/simple-json-datasource/blob/master/README.md
    jsonobj_mean = json.loads(tdms_DF.to_json(orient='split'))

    datapoints_array_mean = []
       
#     print([np.array(jsonobj_mean['data'])[0]])
    
    for i in range(0, tdms_LENGTH):
        datapoints_array_mean.append([(jsonobj_mean['data'])[0][i], TIME_START])
        TIME_START = float(TIME_START) + TIME_DELTA

    
#     print(TIME_START)
    # construct json array for API response
    dict_data_mean = {}
    dict_data_mean["target"] = 'original'
    dict_data_mean["datapoints"] = datapoints_array_mean
    
    jsonarr = json.dumps([dict_data_mean])
    
    
    return str(jsonarr)

def query_timestamp (TYPE, feature, EQU_ID,time_start):
    
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
    mgdb_collection = 'w4_features'

    time_start = time_start.replace("/", "-")
#     print("time_start",time_start)
    time_end = datetime.datetime.strptime(time_start, '%Y-%m-%d') + datetime.timedelta(days=1)
#     print('time_end', type(time_end), time_end)
    time_end = time_end.strftime("%Y-%m-%d")
#     print('time_end', type(time_end), time_end)
    ## Query MongoDB
    measurement, data = read_MongoDB_data(EQU_ID,
                                        host = mgdb_host,
                                      port = mgdb_port,
                                       dbname = mgdb_database,
                                       # ChannelName = ChannelName,
                                       time_start = time_start,
                                       time_end = time_end,
                                       user = mgdb_username,
                                       password = mgdb_password,
                                       DATE = time_start,
                                       
                                          )


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

def convert_equ_name (EQU_NAME):
    
    ## Connection Information
    WISE_PAAS_INSTANCE = 'fomos.csc.com.tw'
    ENDPOINT_SSO = 'portal-sso'
    ENDPOINT_APM = 'api-apm-csc-srp'

    payload = dict()
    payload['username'] = 'william.cheng@advantech.com.tw'
    payload['password'] = 'Tzukai3038!'


    ## Get Token through SSO Login
    resp_sso = requests.post('https://' + ENDPOINT_SSO + '.' + WISE_PAAS_INSTANCE + '/v2.0/auth/native', 
                     json=payload,
                     verify=False)

    header = dict()
    header['content-type'] = 'application/json'
    header['Authorization'] = 'Bearer ' + resp_sso.json()['accessToken']


    ## Get NodeID by EQU_NAME
    APM_NODEID = 'https://' + ENDPOINT_APM + '.' + WISE_PAAS_INSTANCE + '/topo/progeny/node'

    param = dict()
    param['topoName'] = 'MAIN_CSC'
    param['path'] = '/'
    param['type'] = 'layer'
    param['layerName'] = 'Machine'

    resp_apm_nodeid = requests.get(APM_NODEID, 
                     params=param,
                     headers=header,
                     verify=False)

    ## Retrive NodeID
    resp_apm_nodeid_json = resp_apm_nodeid.json()
    node_id_df = pd.DataFrame(resp_apm_nodeid_json)
    node_id_df = node_id_df[['id', 'name']]

    apm_nodeid = int(node_id_df.loc[node_id_df['name'] == EQU_NAME]['id'])
    

    ## Get EQU_ID by NodeID
    APM_TOPO_INFO = 'https://' + ENDPOINT_APM + '.' + WISE_PAAS_INSTANCE + '/topo/node/detail/info'

    param = dict()
    param['id'] = apm_nodeid

    resp_apm_feature = requests.get(APM_TOPO_INFO, 
                     params=param,
                     headers=header,
                     verify=False)

    ## Retrive EQU_ID
    resp_tag = resp_apm_feature.json()['dtInstance']['feature']['monitor']
    feature_list = pd.DataFrame(resp_tag)['tag'].str.split('@', expand=True)[2].sort_values()
    EQU_ID = str(resp_apm_feature.json()['dtInstance']['property']['iotSense']['deviceId']).split('@')[2]

    return EQU_ID

#def convert_bin (filename, pd_type, DISPLAY_POINT):
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
    return_df = return_df.T

    file_length = len(return_df.columns) 
    length = file_length / DISPLAYPOINT

    return return_df, file_length


def read_MongoDB_data(EQU_ID,
                        host = '10.100.10.1',
                       port = '27017',
                       dbname = '2eeb002d-1fcd-44a5-8370-648a43eef634',
                       # ChannelName='1Y520210100',
                       time_start='', 
                       time_end='', 
                       user = '8f28b802-3bfc-4d54-ae71-b21bb69320e2',
                       password = 'KI4j31AE5kUpv4HxgvLphtD26',
                       mgdb_collection = 'w4_features',
                       DATE='',
                       
                       ):
    
    #Example: read_influxdb_data(ChannelName='1Y520210200')
    #Example: read_influxdb_data(ChannelName='1Y520210200',time_start='2018-05-28',time_end='2018-05-29')
    
    client = MongoClient('mongodb://%s:%s@%s/%s' % (user, password, host, dbname))

    db = client[dbname]
    collection = db[mgdb_collection]

    measurement = db.list_collection_names()

    import time
#     DATE ='2019-11-01'
    from datetime import datetime, timedelta
    DATE=datetime.strptime(DATE, "%Y-%m-%d").date()
    print("DATE",DATE)
    import datetime
    DATE_1 = DATE + datetime.timedelta(days=1)

    DATE=str(DATE)
    DATE_1=str(DATE_1)

    pattern = '%Y-%m-%d'
    epoch_DATE = int(time.mktime(time.strptime(DATE, pattern)))
    epoch_DATE_1 = int(time.mktime(time.strptime(DATE_1, pattern)))
    print("epoch_DATE_1",epoch_DATE_1)
    data = pd.DataFrame(list(collection.find({
        "$and":[
            {'timestamp':{"$gte":epoch_DATE}},
            {"timestamp":{"$lte":epoch_DATE_1}},
            {'device':EQU_ID } ] })))
    
    data.index = (pd.to_datetime(data['timestamp'], unit='s'))
    

    return measurement, data

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
