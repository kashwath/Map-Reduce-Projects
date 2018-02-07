# -*- coding: utf-8 -*-
"""
Created on Fri Feb 02 16:00:51 2018

@author: karth
"""

from mrjob.job import MRJob
from mrjob.job import MRStep

class MRGrep(MRJob):
    def steps(self):
        return [MRStep( mapper=self.mapper), MRStep(reducer=self.reducer)]
    
    def configure_options(self):
        super(MRGrep, self).configure_options()
        self.add_passthrough_option(
        '-e', '--expressions', action= 'append', default=[])

    
    def mapper(self, _, line):
        
        for exp in self.options.expressions:
            if(exp in line):
                yield exp, line
        
    def reducer(self, key, values):
            for value in values:
                yield key, value

if __name__ == '__main__':
    MRGrep.run()