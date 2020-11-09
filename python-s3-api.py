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
    # DATE = DATE + datetime.timedelta(hours=8) - datetime.timedelta(days=1)    
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
    #if SPECIFIC_TIME.isdigit() and len(SPECIFIC_TIME) > 3:
    #    print('user specified time and date')
    TS = datetime.datetime.fromtimestamp(int(SPECIFIC_TIME[0:10]))
    #TS = TS + datetime.timedelta(hours=8)
    #    print('Feature assorcated timestamp in Query Date=', TS)
    #else:
        #Dr. Ho: we use specified time instead of search range
        #print('user specified by query')
        #TS = query_timestamp (TYPE, FEATURE, EQU_ID, DATE)
    print('Feature assorcated timestamp in Query Date=', TS)

    # establish connection to s3 and search bin file that the mostest close to query date
    S3_BUCKET = get_s3_bucket()
    
    # parsing EQU_ID to get SMB_ID, for combining S3 Path
    
    DEVICE_NAME = query_device_name (EQU_ID)
    #PATH_DEST = "#1HSM/ROT/vPodPRO/#1內冷式ROT Roller WS_vpod/2020/08/01/"
    PATH_DEST = '#1HSM/ROT/vPodPRO/' + DEVICE_NAME + '/' + str(TS.strftime("%Y")) + '/' + str(TS.strftime("%m")) + '/' + str(TS.strftime("%d")) + '/'

    print("PATH_DEST:",PATH_DEST)
    PATH_DEST=PATH_DEST.encode('utf-8').strip()

    HOUR = str(TS.strftime("%H"))
    MIN = str(TS.strftime("%M"))
    SECOND = str(TS.strftime("%S"))
    #print ('Raw Data-' + DEVICE_NAME + '-' + HOUR + '-'+ MIN + '-' + SECOND + '_25600.bin')
    FILE_NAME = 'Raw Data-' + DEVICE_NAME + '-' + HOUR + '-'+ MIN + '-' + SECOND + '_25600.bin'


    '#1HSM/ROT/vPodPRO/#1內冷式ROT Roller WS_vpod/2020/08/01/'
    'Raw Data-#1內冷式ROT Roller WS_vpod-00-56-26_25600.bin'

    #FILE_NAME = query_file (TS, S3_BUCKET, PATH_DEST, EQU_ID)
    FILE_NAME = 'Raw Data-#1內冷式ROT Roller WS_vpod-00-56-28_25600.bin'

    print("FILE_NAMEx:",FILE_NAME)
    FILE_NAME=FILE_NAME.encode('utf-8').strip()
    print("FILE_NAME:",FILE_NAME)    
    
    # to catch bin file not exist issue, for example: 1Y510110107, 2019-05-21T20:49:55.000Z
    #if FILE_NAME is 'null':
    #    return 'File not found'

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

    #'#1HSM/ROT/vPodPRO/#1內冷式ROT Roller WS_vpod/2020/08/01/'
    #'#1HSM/ROT/vPodPRO/#1內冷式ROT Roller WS_vpod/2020/08/01/'

    #'Raw Data-#1內冷式ROT Roller WS_vpod-00-56-28_25600.bin'
    #'Raw Data-#1內冷式ROT Roller WS_vpod-00-56-26_25600.bin'

    BIN_DF, BIN_LENGTH = convert_bin(FILE_NAME, DISPLAY_POINT)
    if SignalType=='velocity':
        print('velocity change, size from:')
        print(BIN_DF.shape)
        BIN_DF = pd.DataFrame(get_velocity_mms_from_acceleration_g(BIN_DF.values.T,1.0/8192)).T
        print('velocity change, size to:')
        print(BIN_DF.shape)
        print(BIN_DF.values)

    # calculate start-time and end-time for grafana representation
    S3_BUCKET = get_s3_bucket()


    TIME_START = TS.strftime('%Y-%m-%d') + 'T' + HOUR + ':' + MIN + ':' + SECOND
    TIME_START = datetime.datetime.strptime(TIME_START, '%Y-%m-%dT%H:%M:%S')
    TIME_START = TIME_START - datetime.timedelta(hours=8)
    TIME_START = TIME_START.timestamp() * 1000
    TIME_DELTA = float(float(BIN_LENGTH / SAMPLE_RATE) / DISPLAY_POINT) * 1000
    print ('Grafana x-axis TIME_START=', TIME_START)
    print('Datatime='+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # delete file which stored in local
    os.remove(FILE_NAME)

    # combine response json object follow the rule of grafana simpleJSON
    RETURN = combine_return (TIME_START, TIME_DELTA, BIN_DF, BIN_LENGTH)

    return RETURN


def convert_bin (filename, DISPLAYPOINT):
    bytes_read = open(filename, "rb").read()
    size = [hexint(bytes_read[(i*4):((i+1)*4)]) for i in range(2)]
    signal = [struct.unpack('f',bytes_read[(i*4):((i+1)*4)]) for i in range(2,2+size[0]*size[1])]
    data = np.array(signal).reshape(size)

    return_df = pd.DataFrame(data = data)
    return_df = return_df.T
    file_length = len(return_df)

    length = file_length / DISPLAYPOINT

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

def query_file (TS, bucket, PATH_DEST,EQU_ID):
    S3_BUCKET = get_s3_bucket()
    filename = 'tag_list.csv'
    tag_list = os.path.join('/', filename)
    key =S3_BUCKET.get_key(tag_list)
    key.get_contents_to_filename(filename)
    df = pd.read_csv('tag_list.csv', encoding='big5')
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
    for i in range(0, BIN_LENGTH):
        datapoints_array_mean.append([jsonobj_mean['data'][i][0], TIME_START])
        TIME_START = float(TIME_START) + TIME_DELTA

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
                                        time_start = time_start,
                                        time_end = time_end,
                                        user = mgdb_username,
                                        password = mgdb_password,
                                        DATE = time_start)


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
                       DATE=''
                       
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
