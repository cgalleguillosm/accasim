"""
MIT License

Copyright (c) 2017 AlessioNetti, cgalleguillosm

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import socket, json
import os
import argparse
from accasim.utils.misc import CONSTANT, load_config, watcher_daemon

# A simple launch script to contact the watcher demon related to a running test session, and obtain information about
# it. By default the connection is launched on localhost, but the user can choose an IP if the tests are running
# on a different machine.
if __name__ == '__main__':
    CONFIG_FOLDER = '../config/'
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
    #===========================================================================
    # if args.usage:
    #     print('- ' + data['usage'])
    # elif args.globalprogress:
    #     print('- Current test instance: %s' % data['input_filepath'])
    #     print('- Testing file No. of: %i / %i' % (data['number_testfile_now'], data['number_testfiles']))
    #     print('- Test instance on current file No. of: %i / %i' % (data['number_testrun_now'], data['number_testruns']))
    # elif args.localprogress:
    #     print('- Current test instance: %s' % data['input_filepath'])
    #     print('- Completion percentage: %i%%' % int(data['progress'] * 100))
    #     print('- Elapsed time: %i secs' % data['time'])
    # else:
    #     print('- Unknown response received: ')
    #     print(data)
    #===========================================================================

    client_socket.close()
    exit()
