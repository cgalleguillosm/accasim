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
from accasim.utils.reader_class import WorkloadParserBase
from accasim.utils.file import PlainFileReader
from accasim.utils.misc import load_config, type_regexp, DEFAULT_SIMULATION
from re import compile


def define_result_parser(simulator_config=None):
    """
    This function creates a ScheduleParser instance enabled for parsing AccaSim schedule files.

    The objects produced by this function are used for post-processing simulation results.

    :return: A ScheduleParser object
    """
    try:
        if simulator_config is not None:
            _schedule_output = load_config(simulator_config)['schedule_output']
        else:
            # If no simulation config is supplied, DEFAULT_SIMULATION is used
            _schedule_output = DEFAULT_SIMULATION['SCHEDULE_OUTPUT']
        # _separators = _schedule_output['separators']
        _format = _schedule_output['format']
        _attributes = _schedule_output['attributes']
    except KeyError as e:
        print(
            'Schedule output format not identified. Please check the simulator configuration file for the key \'schedule_output\'.')
        exit()

    for _attr_name, _data_type in _attributes.items():
        _format = _format.replace('{' + _attr_name + '}', type_regexp(_data_type[-1]).format(_attr_name))
    return ScheduleParser(_format, [])


class ScheduleParser(WorkloadParserBase):
    """
    This class can read and parse schedule files produced by different sources, by personalizing the underlying parser
    object.

    ScheduleParser class is an implementation of the :class:`accasim.utils.reader_class.WorkloadParserBase`
    """

    def __init__(self, regexp, updater=[]):
        """
        Constructor for the class.

        :param regexp: Regular expression that allows to perform the parsing of lines in the schedule file
        :param updater: A list of functions used by the parser to perform automatic update operations over the lines
            that are read in the schedule file
        """
        WorkloadParserBase.__init__(self)
        self.regexp = compile(regexp)
        self.updater = updater

    def parse_line(self, line):
        """
        This method performs parsing over a single line in the schedule file, and returns a corresponding dict object.

        :param line: The line that must be parsed
        :return: A dict object is successful, None otherwise
        """
        _matches = self.regexp.match(line)
        if not _matches:
            return None
        _dict = _matches.groupdict()
        for u in self.updater:
            u(_dict)
        return _dict


class WorkloadFileReader:
    """
    Class that allows to read and parse schedule files produced by various sources.

    This class employs a ScheduleParser object in order to parse the lines that are read from the schedule file.

    """
    
    def __init__(self, workload, reg_exp, tweak_class, updater=[]):
        """
        Constructor for the class.

        :param workload: Path to the file to be read
        :param reg_exp: Regular expression used to instance the underlying ScheduleParser object
        :param tweak_class: tweak_class instance used to filter the entries read from the schedule file
        :param updater: A list of functions used by the parser to perform automatic update operations over the lines
            that are read in the schedule file
        """
        self.reader = PlainFileReader(workload)
        self.parser = ScheduleParser(reg_exp, updater)
        self.tweak = tweak_class
        
    def next(self, omit_startwith=';'):
        """
        Reads and parses one line from the workload file that is being read.

        :param omit_startwith: All lines starting with this character are skipped, and not returned
        :return: A dictionary corresponding to the read line
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
