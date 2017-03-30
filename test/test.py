#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: winton 
@time: 2017-03-28 16:41 
"""
if __name__ == '__main__':
    with open('1.txt', 'r') as f:
        f.readline()
        # print(type(f.readlines()))
        print(len(f.readlines()))
