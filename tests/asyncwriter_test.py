import os
import sys
import json
from random import randint, random, choice, choices, seed

accasim = os.path.abspath(os.path.join('../../accasim'))
sys.path.insert(0, accasim)

import unittest
from accasim.utils.async_writer import AsyncWriter
from string import ascii_letters, digits

class AsyncWriterTests(unittest.TestCase):
                
    def randomwrite(self, fp, n, v=50):
        seed('async_test')
        writer = AsyncWriter(fp)
        writer.start()
        
        counter = 0
        fake_lines = [''.join(choices(digits + ascii_letters, k=randint(100, 200))) + '\n' for _ in range(v)]
        
        for i in range(n):
            if random() < 0.2:
                continue
            nlines = randint(0, 100)
            counter += nlines
            for _ in range(nlines):
                line = fake_lines[randint(0, v - 1)]
                writer.push(line)
        
        writer.stop()
        return counter
    
    def countlines(self, fp):
        with open(fp) as f:
            return len(list(f))
        
    def deletefile(self, fp):
        os.remove(fp)
    
    def test_randomwrite(self):
        iterations = (50000,)
        fp = 'output/aw_tests.out'
        results = []
        for i in iterations:
            generated_lines = self.randomwrite(fp, i)
            written_lines = self.countlines(fp)
            self.deletefile(fp)
            results.append((generated_lines, written_lines,))
        self.assertTrue(all(t[0] == t[1] for t in results))
            
                
if __name__ == '__main__':
    unittest.main()
        
