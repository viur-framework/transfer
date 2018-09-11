import json, pickle
from sources.viur import NetworkService
from gkey.key import Key
import sys

class fwrap(object):
	def __init__(self, blob, name="", mimetype="application/octet-stream"):
		super( fwrap, self ).__init__()
		self._blob = blob
		self.name = name.encode(sys.getfilesystemencoding())
		self.mimetype = mimetype

	def read(self, *args, **kwargs):
		return( self._blob )

class ViSink(object):
	def __init__(self, cfg):
		super( ViSink, self).__init__()
		if not "url" in cfg["sink-cfg"] or not "importKey" in cfg["sink-cfg"]:
			raise ValueError("\"url\" or \"importKey\" Parameter missing from sink-cfg")
		self.url = cfg["sink-cfg"]["url"]
		self.importKey = cfg["sink-cfg"]["importKey"]
		self.cfg = cfg
		self.ns = NetworkService( self.url )
		self._appId = None

	def listExportModules(self):
		modules = self.ns.decode( self.ns.request("/dbtransfer/listModules", {"key":self.importKey}) )
		res = {}
		for m in modules:
			modulData = self.ns.decode( self.ns.request("/dbtransfer/getCfg/", {"module":m,"key":self.importKey}) )
			res[ m ] = modulData
		return( res )

	def storeEntry(self, modul, entry):
		if not entry:
			return

		id = entry["id"]
		k = Key(encoded=id)
		if k.kind() != modul:
			raise ValueError("Invalid key! Key's kind should be %s, is %s" % (modul,k.kind()))
		if k.app() != self.getAppId():
			raise ValueError("Invalid key! Key's app should be %s, is %s" % (self.getAppId(),k.app()))
		try:
			t = {}
			for k,v in entry.items():
				if isinstance(v,unicode):
					v = v.encode("UTF-8")
				t[k] = v
			self.ns.request("/dbtransfer/storeEntry2", {"e":pickle.dumps(t).encode("HEX"),"key":self.importKey})
		except:
			print("------")
			print( entry )
			raise

	def getAppId(self):
		if self._appId is None:
			self._appId = self.ns.decode( self.ns.request("/dbtransfer/getAppId",{"key":self.importKey}) )
		return( self._appId )

	def storeBlob(self, key, blob, mimetype ):
		ulurl = self.ns.request("/dbtransfer/getUploadURL",{"key":self.importKey} ).decode("UTF-8")
		data = json.loads( self.ns.request(ulurl, {"file": fwrap(blob,"file1", mimetype),"key":self.importKey, "oldkey": key} ).decode("UTF-8") )
		if data["action"] == "addSuccess":
			return( data["values"][0]["dlkey"], mimetype)
		return( key, mimetype )


	def setModulConfig(self, cfg):
		print(cfg)

