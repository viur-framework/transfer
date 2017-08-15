import sqlite3
import pickle

class SqlSink(object):
	def __init__(self, cfg):
		super( SqlSink, self).__init__()
		self.cfg = cfg
		self.conn = sqlite3.connect('example.db')
		self.conn.text_factory = str
		cursor = self.conn.cursor()
		cursor.execute("""Create Table IF NOT EXISTS entries (id text primary key, modul text, changedate text, data blob)""")
		cursor.execute("""Create Table IF NOT EXISTS blobs (id text primary key, data blob)""")
		cursor.execute("""Create Table IF NOT EXISTS modulcfg (id text primary key, data blob)""")
		self.conn.commit()

	def listModules(self):
		return("*")

	def getAppId(self):
		return( None )

	def storeEntry(self, modul, entry):
		id = entry["id"]
		if "changedate" in entry.keys():
			changedate = entry["changedate"]
		else:
			changedate = 1
		cursor = self.conn.cursor()
		cursor.execute("""insert or replace into entries (id,modul,changedate,data) VALUES (?,?,?,?)""", (id,modul,changedate, pickle.dumps(entry)))
		self.conn.commit()

	def storeBlob(self, key, blob ):
		cursor = self.conn.cursor()
		cursor.execute("""insert or replace into blobs (id,data) VALUES (?,?)""", (key, blob))
		self.conn.commit()
		return( key )

	def setModulConfig(self, modulCfg ):
		cursor = self.conn.cursor()
		cursor.execute("""insert or replace into modulcfg (id,data) VALUES (?,?)""", (1, pickle.dumps(modulCfg)))
		self.conn.commit()

	def iterValues(self, modul, lastChangeDate=0):
		cursor = self.conn.cursor()
		for dataStr in cursor.execute("""SELECT data FROM entries WHERE modul=? AND changedate>=?""", (modul, lastChangeDate) ):
			yield( pickle.loads(dataStr[0]) )
		raise StopIteration()