# -*- coding: utf-8 -*-

import sqlite3
import pickle
import pprint
from gkey.key import Key
from utils import StopPropagationException

class SqlSource(object):
	def __init__(self, cfg):
		super(SqlSource, self).__init__()
		if not "file" in cfg["source-cfg"]:
			raise ValueError("\"file\" parameter missing from conf[\"source-cfg\"]")
		self.cfg = cfg
		self.conn = sqlite3.connect(cfg["source-cfg"]["file"])
		self.conn.text_factory = str
		cursor = self.conn.cursor()
		if cursor.execute("""PRAGMA table_info(entries)""").fetchone() is None or \
						cursor.execute("""PRAGMA table_info(blobs)""").fetchone() is None:
			raise ValueError("INVALID INPUT FILE!")
		self.backupRun = 1

	def listExportModules(self):
		raise NotImplementedError()


	def getEntry(self, module, key):
		print "getEntry", module, key
		cursor = self.conn.cursor()
		r = cursor.execute("""SELECT data from entries WHERE id=? and backuprun=?;""",
		                   (key, self.backupRun)).fetchone()
		#if r is None:
		#	r = cursor.execute("""SELECT data from entries WHERE id=?;""",
                #                   (key,)).fetchone()
		#	if r is None:
		#		#raise StopPropagationException()
		#		return None
		#pprint.pprint(pickle.loads(r[0]))
		if r is None:
			raise StopPropagationException()
		return pickle.loads(r[0])

	def getBlob(self, key):
		cursor = self.conn.cursor()
		r = cursor.execute("""SELECT data,mimetype FROM blobs WHERE id=?;""", (key,)).fetchone()
		if r is None:
			raise StopPropagationException()
		return r[0], r[1]

	def iterValues(self, modul, lastChangeDate=0):
		print("itering %s" % modul)
		cursor = self.conn.cursor()
		for dataStr in cursor.execute("""SELECT data FROM entries where backuprun=?;""", (self.backupRun,)):
			data = pickle.loads(dataStr[0])
			if Key(encoded=data["id"]).kind() == modul:
				yield (data)
		raise StopIteration()

