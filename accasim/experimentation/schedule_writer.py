"""
MIT License

Copyright (c) 2017 cgalleguillosm

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
from abc import ABC, abstractclassmethod

from accasim.utils.file import file_exists 

class workload_writer(ABC):
    
    def __init__(self, path, overwrite=False, append=False):
        """

        :param path:
        :param overwrite:
        :param append:
        """
        exists = file_exists(path, True)
        if exists and not (overwrite or append):
            raise Exception('File already exists. Overwrite option is False. Set True to overwrite or change the filename/filepath.')
        if overwrite and append:
            raise Exception('Only one mode (append or overwrite) can be True. ')
                
        if overwrite:
            mode = 'w'
        elif append:
            mode = 'a'
                    
        self.file = open(path, mode)
        
    def add_newline(self, job_dict):
        """

        :param job_dict:
        :return:
        """
        line = self.process_dictionary(job_dict)
        if line[-1] != '\n':
            line += '\n'
        self.file.write(line)

    @abstractclassmethod
    def process_dictionary(self, job_dict):
        """

        :param job_dict:
        :return:
        """
        raise NotImplementedError()
    
    def close_file(self):
        """

        :return:
        """
        self.file.close()
    
    def __del__(self):
        """

        :return:
        """
        if hasattr(self, 'file'):
            self.close_file()
        
class swf_writer(workload_writer):
    
    JOB_NUMBER = ('job_number', 0)
    SUBMIT_TIME = ('submit_time', 0)
    WAIT_TIME = ('wait_time', -1)
    DURATION = ('duration', 0)
    ALLOCATED_PROCESSORS = ('allocated_processors', 0)
    AVG_CPU_TIME = ('avg_cpu_time', -1)
    USED_MEMORY = ('used_memory', 0)
    REQUESTED_NUMBER_PROCESSORS = ('requested_number_processors', -1)
    REQUESTED_TIME = ('requested_time', -1)
    REQUESTED_MEMORY = ('requested_memory', -1)
    STATUS = ('status', 1)
    USER_ID = ('user_id', -1)
    GROUP_ID = ('group_id', -1)
    EXECUTABLE_NUMBER = ('executable_number', -1)
    QUEUE_NUMBER = ('queue_number', -1)
    PARTITION_NUMBER = ('partition_number', -1)
    PRECEDING_JOB_NUMBER = ('preceding_job_number', -1)
    THINK_TIME_PREJOB = ('think_time_prejob', -1)
    SWF_ATTRIBUTES = [
        JOB_NUMBER, SUBMIT_TIME, WAIT_TIME, DURATION, ALLOCATED_PROCESSORS,
        AVG_CPU_TIME, USED_MEMORY, REQUESTED_NUMBER_PROCESSORS, REQUESTED_TIME,
        REQUESTED_MEMORY, STATUS, USER_ID, GROUP_ID, EXECUTABLE_NUMBER,
        QUEUE_NUMBER, PARTITION_NUMBER, PRECEDING_JOB_NUMBER, THINK_TIME_PREJOB
    ]
    
    def __init__(self, path, max_time=14400, overwrite=False, append=False):
        """

        :param path:
        :param max_time:
        :param overwrite:
        :param append:
        """
        workload_writer.__init__(self, path, overwrite, append)
        self.max_time = max_time
        
    def process_dictionary(self, job_dict):
        """

        :param job_dict:
        :return:
        """
        line = [str(self.prepare_data(job_dict, attr_name, default_value)) for attr_name, default_value in self.SWF_ATTRIBUTES]
        return '\t'.join(line) 
        
    def prepare_data(self, job_dict, attr_name, default_value):
        """

        :param job_dict:
        :param attr_name:
        :param default_value:
        :return:
        """
        total_processors = job_dict['resources']['core'] * job_dict['nodes']
        total_mem = job_dict['resources']['mem'] * job_dict['nodes']
        
        job_dict['requested_number_processors'] = total_processors 
        job_dict['allocated_processors'] = total_processors
        job_dict['requested_memory'] = total_mem
        job_dict['used_memory'] = total_mem
        job_dict['requested_time'] = self.max_time
        
        if attr_name in job_dict:
            return job_dict[attr_name]
        return default_value