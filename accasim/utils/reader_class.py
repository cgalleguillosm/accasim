import re
from abc import abstractmethod, ABC
import sys

class workload_parser:
    def __init__(self, reg_exp_dict, avoid_tokens=None):
        """
            workloader_paser is a general class for parsing workload files.
            @param reg_exp: Dictionary where the name of the group is the key and the value
                its regular expresion. It must contain all the regular expresions in a sorted way,
                for appending one at the end of the previous and then recover the value(s) for each group.
            @param avoid_token: List of reg_exp to avoid reading lines. The lines that are avoided 
                won't be readed by the parser. 
        """
        assert(isinstance(reg_exp_dict, dict)), 'The regular expressions must be passed as dictionary. Groupname: Regular Exp'
        self.reg_exp_dict = reg_exp_dict
        assert(isinstance(avoid_tokens, (list, tuple))), 'The tokens to avoid (also regular expressions) must be passed in a list'
        self.avoid_tokens = avoid_tokens
        
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
    def __init__(self, filepath, parser):
        self.last_line = 0
        self.filepath = filepath
        self.parser = parser
        self.file = None
        self.EOF = True
        
    def __del__(self):
        if self.file is not None:
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
                if tmp_lines == n_lines:
                    break
            if tmp_lines < n_lines:
                self.EOF = True
            if self.EOF and tmp_lines == 0:
                return None 
            return lines
        return None
    
    def next_dicts(self, n_lines):
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
        return None if not _dicts else _dicts 