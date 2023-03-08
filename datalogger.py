import socketio
from multiprocessing.pool import ThreadPool
from threading import Timer
import os
from pathlib import Path
import h5py
import numpy as np
import yaml
from epics import PV
import time
from collections import defaultdict

def load_config(config_file):
    """Returns the config file as a dictionary

    Args:
        config_file (str): Path to the configuration file

    Returns:
        dict: Configuration
    """
    with open(config_file, 'r') as _f:
        cfg = yaml.safe_load(_f)
        
    return cfg

def data_file_exists(path, pvname):
    """Checks if the h5 file exists for the specified PV.

    Args:
        path (str): Path to the data folder
        pvname (str): Name of the PV

    Returns:
        bool: True if data file exists, otherwise False
    """
    file_path = Path(path+pvname+'.h5')
    # print(file_path)
    return file_path.exists()

def create_data_file(data_folder_path, pvname, dtypes):
    """Creates the h5 file for the PV in the data path given.

    Args:
        data_folder_path (str): Path to the data folder
        pvname (str): Name of the PV
        dtype (str): Data type
    """
    f = h5py.File(data_folder_path+pvname+'.h5', 'w')
    arr = np.zeros(1)  # dummy data
    f.create_dataset('value', data=arr, dtype=dtypes[0], chunks=True, maxshape=(None,))
    f.create_dataset('time', data=arr, dtype=dtypes[1], chunks=True, maxshape=(None,))
    f.close()

# Load the configuration
config_file = 'example_config.yaml'
config = load_config(config_file)

# Server information
server_address = config['server_address']
server_port = config['server_port']
server_namespace = config['server_namespace']

# Data storage options
BUFFER_SIZE = config['buffer_size']
data_path = config['data_path']
Path(data_path).mkdir(parents=True, exist_ok=True)
pvs = config['pvs']
pv_names = list(pvs.keys())
buffers = {}

def continuous_scan(pvname):
    global buffers
    
    value_dtype = pvs[pvname]['value_dtype']
    time_dtype = pvs[pvname]['time_dtype']
    # Check if the data file exists
    if not data_file_exists(data_path, pvname):
        print('File for {} does not exist. Attempting to create it.'.format(pvname))
        # Create the file, group, and value/time datasets if it does not
        create_data_file(data_folder_path=data_path, pvname=pvname, dtypes=(value_dtype, time_dtype))

    # Initialize the buffers and packet index
    value_buffer = np.zeros(BUFFER_SIZE, dtype=value_dtype)
    time_buffer = np.zeros(BUFFER_SIZE, dtype=time_dtype)
    packet_index = 0
    
    # Write to the global buffer
    buffers[pvname] = {'value': value_buffer, 'time': time_buffer, 'idx': packet_index}
    
    # Create the socket client and connect to the pvServer
    sio = socketio.Client()
    sio.connect('http://'+server_address+':'+str(server_port), namespaces=[server_namespace])
    sio.emit('request_pv_info', {'data': pvname}, namespace=server_namespace)  # Set up the PV on the server

    @sio.on(pvname, namespace=server_namespace)
    def handle_msg(data):
        # print(data)
        if 'pvname' in data.keys():
            pvname = data['pvname']
        else:
            return 1
        
        i = buffers[pvname]['idx']

        # check if the buffer is full and save if it is
        if i == BUFFER_SIZE:
            h5file = h5py.File(data_path+pvname+'.h5', 'a')
            value_dataset = h5file['value']
            time_dataset = h5file['time']
            
            value_dataset.resize(value_dataset.shape[0] + BUFFER_SIZE, axis=0)
            value_dataset[-BUFFER_SIZE:] = buffers[pvname]['value']
            time_dataset.resize(time_dataset.shape[0] + BUFFER_SIZE, axis=0)
            time_dataset[-BUFFER_SIZE:] = buffers[pvname]['time']
            
            buffers[pvname]['idx'] = 0
            i = 0

            h5file.close()
        
        # check if the data is good, if it is then process it
        if 'value' in data.keys() and 'timestamp' in data.keys():
            val = float(data['value'])
            timestamp = float(data['timestamp'])
            
            buffers[pvname]['value'][i] = val
            buffers[pvname]['time'][i] = timestamp
            buffers[pvname]['idx'] += 1
    
    @sio.event
    def disconnect():
        # Write the rest of the buffer to file
        print('Saving the rest of the buffer to disk.')
        h5file = h5py.File(data_path+pvname+'.h5', 'a')
        value_dataset = h5file['value']
        time_dataset = h5file['time']
        
        idx = buffers[pvname]['idx']
        
        value_dataset.resize(value_dataset.shape[0] + idx, axis=0)
        value_dataset[-idx:] = buffers[pvname]['value']
        time_dataset.resize(time_dataset.shape[0] + idx, axis=0)
        time_dataset[-idx:] = buffers[pvname]['time']
        
        h5file.close()

    sio.wait()

timer_queue = []
sample_rates = {}
continuous_pvnames = []
for pvname in pv_names:
    scan_type = pvs[pvname]['scan_type']
    if scan_type == 'sample':
        sample_rates[pvname] = pvs[pvname]['sample_rate']
    elif scan_type == 'continuous':
        continuous_pvnames.append(pvname)

def sample_scan(pv, pvname):
    global buffers
    global timer_queue
    
    if pv.connected:
        i = buffers[pvname]['idx']
        buffers[pvname]['value'][i] = pv.value
        buffers[pvname]['time'][i] = pv.timestamp
        buffers[pvname]['idx'] += 1
    else:
        pass
    timer_queue.append(pvname)

pyepics_pvs = {}
for pvname in sample_rates.keys():
    # try a couple times
    n = 0
    pv = PV(pvname)
    while n < 10 or not pv.connected:
        pv = PV(pvname)
        time.sleep(0.01)
        n += 1
    if not pv.connected:
        print('PV {} did not connect!'.format(pvname))
    pyepics_pvs[pvname] = pv
    timer_queue.append(pvname)

    value_dtype = pvs[pvname]['value_dtype']
    time_dtype = pvs[pvname]['time_dtype']

    value_buffer = np.zeros(BUFFER_SIZE, dtype=value_dtype)
    time_buffer = np.zeros(BUFFER_SIZE, dtype=time_dtype)
    buffers[pvname] = {'value': value_buffer, 'time': time_buffer, 'idx': 0}
    
    if not data_file_exists(data_path, pvname):
        print('File for {} does not exist. Attempting to create it.'.format(pvname))
        create_data_file(data_folder_path=data_path, pvname=pvname, dtypes=(value_dtype, time_dtype))

if len(continuous_pvnames) > 0:
    t = ThreadPool(len(continuous_pvnames))
    t.map(continuous_scan, continuous_pvnames)

while True:
    for pvname in timer_queue:
        
        # check if the buffer is full
        if buffers[pvname]['idx'] == BUFFER_SIZE:
            h5file = h5py.File(data_path+pvname+'.h5', 'a')
            value_dataset = h5file['value']
            time_dataset = h5file['time']
            
            value_dataset.resize(value_dataset.shape[0] + BUFFER_SIZE, axis=0)
            value_dataset[-BUFFER_SIZE:] = buffers[pvname]['value']
            time_dataset.resize(time_dataset.shape[0] + BUFFER_SIZE, axis=0)
            time_dataset[-BUFFER_SIZE:] = buffers[pvname]['time']
            
            buffers[pvname]['idx'] = 0
            h5file.close()
        
        timer = Timer(sample_rates[pvname], sample_scan, args=(pyepics_pvs[pvname], pvname))
        timer.start()
        
        timer_queue.remove(pvname)
