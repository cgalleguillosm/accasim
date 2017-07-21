import socket, json
import os
import argparse
from events_simulator.utils.misc import CONSTANT, load_config, watcher_demon

# A simple launch script to contact the watcher demon related to a running test session, and obtain information about
# it. By default the connection is launched on localhost, but the user can choose an IP if the tests are running
# on a different machine.
if __name__ == '__main__':
    CONFIG_FOLDER = 'config/'
    ESSENTIALS_FILENAME = 'essentials.config'
    const = CONSTANT()
    const.load_constants(load_config(os.path.join(CONFIG_FOLDER, ESSENTIALS_FILENAME)))

    parser = argparse.ArgumentParser(description="Client For HPC Simulator Watcher Daemon")
    parser.add_argument("-usage", action="store_true", dest="usage", help="Request current virtual resource usage.")
    parser.add_argument("-progress", action="store_true", dest="progress", help="Request current local progress.")
    parser.add_argument("-all", action="store_true", dest="all", help="Request all previous data.")
    parser.add_argument("-ip", action="store", dest="hostip", default="localhost", type=str, help="IP of server machine.")

    args = parser.parse_args()

    # Remember that commands and responses must not be longer than MAX_LENGTH!
    command = ''
    if args.usage:
        command = 'usage'
    elif args.progress:
        command = 'progress'
    elif args.all:
        command = 'all'
    else:
        print('No commands specified. Quitting...')
        exit()

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((args.hostip, const.WATCH_PORT))
    client_socket.sendall(json.dumps(command).encode())
    data = json.loads(client_socket.recv(watcher_demon.MAX_LENGTH).decode())

    if 'input_filepath' in data:
        print('- Current test instance: {}'.format(data['input_filepath']))
    if 'progress' in data:
        print('\t Completion percentage: {:.2f}%'.format(data['progress'] * 100))
    if 'actual_time' in data:
        print("\t Current simulated time : {}".format(data['actual_time']))
    if all(key in data for key in ['number_testrun_now', 'number_testruns']):
        print('\t Test instance on current file No. of: {} / {}'.format(data['number_testrun_now'], data['number_testruns']))
    if 'simulation_status' in data:
        print('\t {}'.format(data['simulation_status']))
    if 'usage' in data:
        print('\t {}'.format(data['usage']))
    if 'time' in data:
        print("\t Real elapsed time : {:.2f} secs".format(data['time']))

    client_socket.close()
    exit()
