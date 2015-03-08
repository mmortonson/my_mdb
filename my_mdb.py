#!/usr/bin/env python


import sqlite3


class InputParser(object):
    def __init__(self):
        self.input = []

    def read_input(self, prompt=''):
        input_string = raw_input(prompt)
        self.input = input_string.split()

    def get_input(self, i=None):
        if i is None:
            return self.input
        else:
            return self.input[i]

    def has_input(self):
        return len(self.input) > 0


if __name__ == '__main__':
    input_parser = InputParser()
    while not input_parser.has_input() or \
            (input_parser.has_input() and
             not input_parser.get_input(0) in ('exit', 'quit', 'q')):
        input_parser.read_input()
