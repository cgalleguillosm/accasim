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
from accasim.utils.reader_class import workload_parser_base
from accasim.utils.file import plain_file_reader
from accasim.utils.misc import load_config, type_regexp, DEFAULT_SIMULATION
from re import compile as _compile


def define_result_parser(simulator_config=None):
    """

    :return:
    """
    try:
        if simulator_config is not None:
            _schedule_output = load_config(simulator_config)['schedule_output']
        else:
            # If no simulation config is supplied, DEFAULT_SIMULATION is used
            _schedule_output = DEFAULT_SIMULATION().parameters['SCHEDULE_OUTPUT']
        # _separators = _schedule_output['separators']
        _format = _schedule_output['format']
        _attributes = _schedule_output['attributes']
    except KeyError as e:
        print(
            'Schedule output format not identified. Please check the simulator configuration file for the key \'schedule_output\'.')

    for _attr_name, _data_type in _attributes.items():
        _format = _format.replace('{' + _attr_name + '}', type_regexp(_data_type[-1]).format(_attr_name))
    return schedule_parser(_format, [])

class schedule_parser(workload_parser_base):
    def __init__(self, regexp, updater=[]):
        """
        schedule_parser class is an implementation of the :class:`accasim.utils.reader_class.workload_parser_base`
         
        :param regexp:
        
        """
        workload_parser_base.__init__(self)
        self.regexp = _compile(regexp)
        self.updater = updater

    def parse_line(self, line):
        """

        :param line:
        :return:
        """
        _matches = self.regexp.match(line)
        if not _matches:
            return None
        _dict = _matches.groupdict()
        for u in self.updater:
            u(_dict)
        return _dict
    
class workload_file_reader:
    
    def __init__(self, workload, reg_exp, tweak_class, updater=[]):
        """

        :param workload:
        :param reg_exp:
        :param tweak_class:
        :param updater:
        """
        self.reader = plain_file_reader(workload)
        self.parser = schedule_parser(reg_exp, updater)
        self.tweak = tweak_class
        
    def next(self, omit_startwith=';'):
        """

        :param omit_startwith:
        :return:
        """
        if self.reader.EOF:
            return None
        line = self.reader.nextline()
        if not line or line.isspace():
            return None
        try:
            while omit_startwith in line:
                line = self.reader.nextline()
                if not line:
                    return None
        except TypeError as e:
            print('Error line: {}'.format(line))
            print(e)
            exit()
        parsed_line = self.parser.parse_line(line)
        return self.tweak.tweak_function(parsed_line)
