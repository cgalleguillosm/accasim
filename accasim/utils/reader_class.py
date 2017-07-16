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
import re
from abc import abstractmethod, ABC
from accasim.utils.misc import CONSTANT, default_swf_parse_config
import sys
from builtins import issubclass

class workload_parser_base(ABC):
    
    @abstractmethod
    def parse_line(self, line):
        pass
    
class default_workload_parser(workload_parser_base):
    def __init__(self):
        workload_parser_base.__init__(self)
        """
            workloader_paser is a general class for parsing workload files.
            @param reg_exp: Dictionary where the name of the group is the key and the value
                its regular expresion. It must contain all the regular expresions in a sorted way,
                for appending one at the end of the previous and then recover the value(s) for each group.
            @param avoid_token: List of reg_exp to avoid reading lines. The lines that are avoided 
                won't be readed by the parser. 
        """
        self.reg_exp_dict, self.avoid_tokens = default_swf_parse_config
        
    def feasible_line(self, line):
        """
            @param line: Line to be checked
            @return: True if it can be parse (it does not match to any avoid token), False otherwise.
        """
        for _token in self.avoid_tokens:
            p = re.compile(_token)
            if p.fullmatch(line):
                return False
        return True        
    
    def parse_line(self, line):
        """
            Parse a feasible line, returning a dict for all groupnames
            @param line: Line to be checked
        """
        if not self.feasible_line(line):
            return None
        reg_exp = r''
        for _key, _reg_exp in self.reg_exp_dict.items():
            reg_exp += _reg_exp[0].format(_key)
        p = re.compile(reg_exp)
        _matches = p.match(line)
        if not _matches:
            return None
        return {k:self.reg_exp_dict[k][1](v) for k, v in _matches.groupdict().items()}

class reader:
    def __init__(self, filepath, parser=None, max_lines=None):
        self.const = CONSTANT()
        if parser:
            if not isinstance(parser, workload_parser_base):
                assert(issubclass(parser, workload_parser_base)), 'Only default_workload_parser class can be used as parsers'
                self.parser = parser()
            else:
                assert(isinstance(parser, workload_parser_base)), 'Only default_workload_parser object can be used as parsers'
                self.parser = parser                
        else:
            self.parser = default_workload_parser()
        self.last_line = 0
        self.max_lines = max_lines
        self.filepath = filepath
        self.file = None
        self.EOF = True
        
    def __del__(self):
        if self.file:
            self.file.close()
            self.file = None   
            self.EOF = True 
        
    def open_file(self): 
        if self.file is None:
            self.file = open(self.filepath)
            self.EOF = False    
        return self.file
    
    def read_next_lines(self, n_lines=1):
        # logging.debug('Reading next %i lines.' % (n_lines))
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
            if self.EOF and tmp_lines == 0:
                return None 
            return lines
        return None
    
    def next_dicts(self, n_lines=1):
        """
            @param n_lines: number of lines to be parsed
            @return: return a list of dicts corresponding to the lines   
        """    
        _dicts = []
        while len(_dicts) < n_lines:
            line = self.read_next_lines()
            # No more lines. End of File
            if not line:
                break
            _dict = self.parser.parse_line(line[0])
            if _dict:
                _dicts.append(_dict)
            elif self.max_lines:
                if self.max_lines == self.last_line:
                    self.EOF = False
                self.max_lines += 1
        return None if not _dicts else _dicts
    
class default_reader(reader):
    pass 