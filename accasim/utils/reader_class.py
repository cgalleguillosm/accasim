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

from abc import abstractmethod, ABC
from re import compile

from accasim.utils.misc import CONSTANT, DEFAULT_SWF_PARSE_CONFIG, obj_assertion
from accasim.base.resource_manager_class import Resources


class WorkloadParserBase(ABC):
    """
    
    Workload Parser Abstract class 
    
    """    
    
    @abstractmethod
    def parse_line(self, line):
        """
        
        Parses a lines and retrieves the corresponding dictionary.
        
        :param line: A text line which keep all job data.
        
        :return: A dictionary of the parsed data. 
        """
        raise NotImplementedError()

    
class DefaultWorkloadParser(WorkloadParserBase):

    def __init__(self):
        WorkloadParserBase.__init__(self)
        """
            workloader_paser is a general class for parsing workload files.
            :param reg_exp: Dictionary where the name of the group is the key and the value
                its regular expresion. It must contain all the regular expresions in a sorted way,
                for appending one at the end of the previous and then recover the value(s) for each group.
            :param avoid_token: List of reg_exp to avoid reading lines. The lines that are avoided 
                won't be readed by the parser. 
        """
        self.reg_exp, self.avoid_tokens = DEFAULT_SWF_PARSE_CONFIG
        self.reg_exp_dict = {}
        self._compile_job_regexp()
        self._compile_infeasible_regexp()
        
    def feasible_line(self, line):
        """
            :param line: Line to be checked
            :return: True if it can be parse (it does not match to any avoid token), False otherwise.
        """
        for p in self._compiled_infeasible_regexp:
            if p.fullmatch(line):
                return False
        return True        
    
    def parse_line(self, line):
        """
            Parse a feasible line, returning a dict for all groupnames
            :param line: Line to be parsed
            
            :return: A dictionary of the parsed data.
            
        """
        if not self.feasible_line(line):
            return None
        _matches = self._compiled_job_regexp.match(line)
        if not _matches:
            return None
        _dict = {k:self.reg_exp_dict[k][1](v) for k, v in _matches.groupdict().items()}
        
        if _dict['requested_number_processors'] != -1:
            _dict['total_processors'] = _dict.pop('requested_number_processors')
        elif  _dict['allocated_processors'] != -1:
            _dict['total_processors'] = _dict.pop('allocated_processors')
        else:
            return None
        if _dict['requested_memory'] != -1:
            _dict['mem'] = _dict.pop('requested_memory')
        elif _dict['used_memory'] != -1:  
            _dict['mem'] = _dict.pop('used_memory')
        else:
            pass 
        return _dict

    def _compile_job_regexp(self):
        reg_exp = r''
        for (_key, _reg_exp) in self.reg_exp:
            self.reg_exp_dict[_key] = _reg_exp
            reg_exp += _reg_exp[0].format(_key)
        self._compiled_job_regexp = compile(reg_exp)
        
    def _compile_infeasible_regexp(self):
        self._compiled_infeasible_regexp = []
        for _token in self.avoid_tokens:
            self._compiled_infeasible_regexp.append(compile(_token))


class Reader(ABC):
    """
    This class is used to simulate the creation of jobs from HPC users.
    This is an abstract class. The main method is read, which must be implemented to return the set of next submission for the system.
    
    :Note:
    
        A default implementation is named as DefaultReader. This class read from a single file, and use a SWF parser to extract the jobs.
    
    """
    
    def __init__(self, _job_factory):
        """
        
        Reader class constructor.
        
        :Note:
        
            For a job generation at least the required attributes for a job event must be presented: *job_id*, *queued_time*, *duration*.
        
        :param _job_factory: Job Factory instance.
        
        
        """
        self.last_time = None
        self.job_factory = _job_factory
        self.submission_enabled = True
        self.loaded_jobs = []
    
    def next(self, current_time, time_points=2, stime_name='queued_time'):
        """
        
        Laods the data and generates the jobs that belongs to the corresponding next `time_points`.
        
        :param current_time: Current simulated point.
        :param time_points: Number of submission points to be loaded
        :param stime_name: Name of the attribute (key dictionary) of the submit/queue time
        
        :return: A tuple composed with an array of the next time points sorted chronologically and a Dictionary with an array for each time point {time_point: [job_1, ..., job_n]} 
        
        """
        #=======================================================================
        # For each time point a list of dictionary of jobs will maintained in 
        # the next_jobs variable.
        #=======================================================================
        time_samples = time_points
        next_points, next_jobs = self._reload_jobs(current_time, stime_name)
        while self.submission_enabled and time_samples >= 0:
            _dict = self._read(current_time)
            if not _dict:
                continue
            if self.last_time != _dict[stime_name]:
                self.last_time = _dict[stime_name]
                next_points.append(self.last_time)
                time_samples -= 1
                
            if self.last_time not in next_jobs:
                next_jobs[self.last_time] = []
                
            next_jobs[self.last_time].append(self.job_factory.factory(**_dict))
        if len(next_jobs) > time_points:
            self.loaded_jobs = next_jobs.pop(self.last_time, [])
            next_points.pop()                
            
        return (next_points, next_jobs)
    
    def _reload_jobs(self, current_time, stime_name):
        """
        
        Takes the already loaded jobs from a previous load process which exceeded the time steps requested.
        
        :param current_time:  Current simulated point.
        :param stime_name: Name of the attribute (key dictionary) of the submit/queue time
        
        :return: A tuple of an array of the previous time points and a dictionary with jobs for each time point.
        """
        jobs_dict = {}
        time_points = []
        for _job in self.loaded_jobs:
            _subt_job = getattr(_job, stime_name)
            assert(_subt_job >= current_time), 'Error of the loading jobs process. The submission time belongs to the past. ({} is previous to {})'.format(_subt_job, current_time)
            if _subt_job not in jobs_dict:
                jobs_dict[_subt_job] = []
                time_points.append(_subt_job)
            jobs_dict[_subt_job].append(_job)
        self.loaded_jobs = []
        return (time_points, jobs_dict)
    
    @abstractmethod
    def _read(self, current_time):
        """
        
        This method must return a dictionary with all the required keys for covering the job attributes. 
        None must be returned If the data is not valid or incomplete.   
        
        :param current_time: The current time if it's needed
        
        :return: A job's dictionary
        
        """
        raise NotImplementedError()
    
    def stop_submission(self):
        """
        
        Marks as stopped the submission process. 
        
        """
        self.submission_enabled = False

    
class DefaultReader(Reader):
    """
    
    A default implementation of the reader class. 
    This implementation works reading a workload file line by line.
    
    
    """

    def __init__(self, filepath, job_factory=None, parser=None, tweak_function=None, max_lines=None, start_time=0, equivalence={}):
        """
        Class constructor
        
        :param filepath: Filepath to the workload file.
        :param job_factory: A :class:`.job_factory` object
        :param parser: An implementation of :class:`.WorkloadParserBase` object. By default, :class:`.DefaultWorkloadParser` is used to handle SWF files. 
        :param tweak_function: Function that allows to tweak a dictionary.
        :param max_lines: Optional. Number of lines to read. None for reading the entire file.
        :param equivalence: Optional. Transforms from workload format a key:value to a new key with a new value in regards of the equivalence.
        
        """
        Reader.__init__(self, job_factory)
        self.parser = None
        self.tweak_function = None
        if parser:
            if not isinstance(parser, WorkloadParserBase):
                assert(issubclass(parser, WorkloadParserBase)), 'Only :class:`.WorkloadParserBase` class can be used as parsers'
                self.parser = parser()
            else:
                assert(isinstance(parser, WorkloadParserBase)), 'Only :class:`.WorkloadParserBase` object can be used as parsers'
                self.parser = parser                
        else:
            self.parser = DefaultWorkloadParser()
            if not tweak_function:
                _resources = self.job_factory.resource_manager.system_resources()
                self.tweak_function = DefaultTweaker(start_time, _resources, equivalence)

        if tweak_function:
            assert(isinstance(tweak_function, Tweaker)), 'The tweak_function argument must be an implementation of the :class:`.Tweaker`'
            self.tweak_function = tweak_function
        elif not self.tweak_function:
            self.tweak_function = None

        self.equivalence = equivalence
        self.start_time = start_time
        self.last_line = 0
        self.max_lines = max_lines
        self.filepath = filepath
        self.file = None
        self.EOF = True
        self.open_file()
        
    def __del__(self):
        """

        :return:
        """
        if hasattr(self, 'file') and self.file:
            self.file.close()
            self.file = None   
            self.EOF = True 
        
    def open_file(self):
        """

        :return:
        """
        if self.file is None:
            self.file = open(self.filepath)
            self.EOF = False    
        return self.file
    
    def _read_next_lines(self, n_lines=1):
        """

        :param n_lines:
        :return:
        """
        if not self.EOF:
            tmp_lines = 0
            lines = [] 
            for line in self.file:
                lines.append(line[:-1])
                self.last_line += 1
                tmp_lines += 1
                if tmp_lines == n_lines or (self.max_lines and self.max_lines == self.last_line):
                    break
            if tmp_lines < n_lines or (self.max_lines and self.max_lines == self.last_line):
                self.EOF = True
                self.stop_submission()
            if self.EOF and tmp_lines == 0:
                return None 
            return lines
        return None
    
    def _read(self, current_time=0):
        """

        :param current_time:
        :return:
        """
        line = self._read_next_lines()
        # No more lines. End of File
        if not line:
            return None
        parsed_line = self.parser.parse_line(line[0])
        if not parsed_line:
            return None
        if self.tweak_function: 
            parsed_line = self.tweak_function.tweak_function(parsed_line)
        return parsed_line


class Tweaker(ABC):
    
    def __init__(self, **kwargs):
        """

        :param kwargs:
        """
        pass
    
    @abstractmethod
    def tweak_function(self, job_dict):
        """

        :param job_dict:
        :return:
        """
        pass


class DefaultTweaker(Tweaker):

    def __init__(self, start_time, system_resources=None, equivalence=None):
        """

        :param start_time:
        :param equivalence:
        """
        if system_resources:
            obj_assertion(system_resources, Resources)
        self.start_time = start_time
        self.equivalence = equivalence if equivalence else {'processor': {'core': 1}}       
        
    def tweak_function(self, _dict):
        """
        
        As in the SWF workload logs the numbers of cores are not expressed, just the number of requested processors, we have to tweak this information
        i.e we replace the number of processors by the number of requested cores.        
        
        The equivalence from processor to core is given in the system config file. As in the example, one processor contains two cores. Then the number of cores will be
        processor \* core. Besides, memory is expressed in kB per processor. 
        
        :Example:
            
        >>> "equivalence": {
        >>>     "processor": {
        >>>         "core": 2
        >>>     }
        >>> }
        
        :param _dict: Dictionary to be tweaked.
        
        :return: The tweaked dictionary.
        
        """
        _processors = _dict['total_processors']
        
        _dict['requested_resources'] = {}
        for k, v in self.equivalence.items():
            #===================================================================
            # Since SWF doesn't provide any information about nodes request
            # every single processor request is considered as a node request.
            # An specific tweak class has to be implemented to specify a conversion of this value. 
            #===================================================================
            if k == 'processor':
                for k2, v2 in v.items():
                    _dict[k2] = v2 * _processors
                    _dict['requested_resources'][k2] = v2
        _dict['requested_nodes'] = _processors
        if 'mem' in _dict:
            _dict['requested_resources']['mem'] = _dict['mem'] 
            _dict['mem'] = _dict['mem'] * _processors
        _dict['queue'] = _dict.pop('queue_number')
                
        _dict['queued_time'] = _dict.pop('queued_time') + self.start_time
        
        assert (
        _dict['core'] >= 0), 'Please consider to clean your data cannot exists requests with any info about core request.'
        if 'mem' in _dict:
            assert (
            _dict['mem'] >= 0), 'Please consider to clean your data cannot exists requests with any info about mem request.'
        return _dict
