# -*- coding: utf-8 -*-

import json

conf = {}

def load(file):
	global conf
	conf = json.loads(open(file, "r").read())
