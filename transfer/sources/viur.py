import urllib2
from urllib import quote_plus
import pickle, json, random, mimetypes, string, sys, os
from time import sleep
from utils import StopPropagationException
import  pprint
import magic

class NetworkService( object ):
	baseURL = None
	def __init__( self,  baseURL ):
		super( NetworkService, self ).__init__()
		self.baseURL = baseURL
		cp = urllib2.HTTPCookieProcessor()
		self.opener = urllib2.build_opener( cp )
		urllib2.install_opener( self.opener )



	@staticmethod
	def genReqStr( params ):
		boundary_str = "---"+''.join( [ random.choice(string.ascii_lowercase+string.ascii_uppercase + string.digits) for x in range(13) ] )
		boundary = boundary_str.encode("UTF-8")
		res = b'Content-Type: multipart/mixed; boundary="'+boundary+b'"\nMIME-Version: 1.0\n'
		res += b'\n--'+boundary
		for(key, value) in list(params.items()):
			if all( [x in dir( value ) for x in ["name", "read", "mimetype"] ] ): #File
				dataVal = value.read()
				type = value.mimetype
				if type=="application/octet-stream":
					type = magic.from_buffer(dataVal, mime=True)
				res += b'\nContent-Type: '+type.encode("UTF-8")+b'\nMIME-Version: 1.0\nContent-Disposition: form-data; name="'+key.encode("UTF-8")+b'"; filename="'+os.path.basename(value.name).decode(sys.getfilesystemencoding()).encode("UTF-8")+b'"\n\n'
				res += dataVal
				res += b'\n--'+boundary
			elif isinstance( value, list ):
				for val in value:
					res += b'\nContent-Type: application/octet-stream\nMIME-Version: 1.0\nContent-Disposition: form-data; name="'+key.encode("UTF-8")+b'"\n\n'
					if isinstance( val, unicode ):
						res += val.encode("UTF-8")
					else:
						res += str(val)
					res += b'\n--'+boundary
			else:
				res += b'\nContent-Type: application/octet-stream\nMIME-Version: 1.0\nContent-Disposition: form-data; name="'+key.encode("UTF-8")+b'"\n\n'
				if isinstance( value, unicode ):
					res += unicode( value ).encode("UTF-8")
				else:
					res += str( value )
				res += b'\n--'+boundary
		res += b'--\n'
		return( res, boundary )

	def request( self, url, params=None, secure=False, extraHeaders=None ):
		def doReq( self, url, params, secure, extraHeaders ):
			if secure:
				skey = json.loads( urllib2.urlopen( self.baseURL+ "/skey" ).read() )
				if params is None or isinstance(params,bytes):
					if "?" in url:
						url += "&skey="+skey
					else:
						url += "?skey="+skey
				else:
					params["skey"] = skey
			if not url.startswith("http"):
				rurl = self.baseURL+url
			else:
				rurl = url
			if isinstance( params,  dict ):
				res, boundary = self.genReqStr( params )
				r = urllib2.Request( rurl.encode("UTF-8"), res, headers={b"Content-Type": b'multipart/form-data; boundary='+boundary+b'; charset=utf-8'})
				req = urllib2.urlopen( r )
			else:
				req = urllib2.urlopen( rurl )

			return( req.read() )

		for x in range(0,4):
			try:
				return( doReq( self, url, params, secure, extraHeaders ) )
			except:
				if x == 3:
					print("Final, fatal error calling Network")
					raise
				print("Error calling network.. Sleeping 60 Seconds")
				sleep(60)

	def decode(self, data):
		return( pickle.loads( data ) )

class ViSource( object ):
	def __init__(self, cfg):
		super( ViSource, self ).__init__()
		self.url = "fixme"
		#self.url = "http://localhost:8080"
		self.exportKey = "fixme"
		self.cfg = cfg
		self.ns = NetworkService( self.url )

	def listExportModules(self):
		modules = self.ns.decode( self.ns.request("/dbtransfer/listModules", {"key":self.exportKey}) )
		res = {}
		for m in modules:
			modulData = self.ns.decode( self.ns.request("/dbtransfer/cfg/", {"modul":m, "key":self.exportKey}) )
			res[ m ] = modulData
		import sys, pprint
		pprint.pprint(res)
		sys.exit(1)
		return( res )

	def getBlob(self, blobKey ):
		return(self.ns.request("/file/download/%s" %blobKey ) )

	def getLastChangeDate(self, modul):
		return( self.ns.decode( self.ns.request("/dbtransfer/getLastChangeDate", {"modul":modul, "key":self.exportKey}) ) )

	def iterValues(self, modul, lastChangeDate=0):
		reqDict = {"module":modul, "key":self.exportKey }
		if lastChangeDate:
			reqDict["lastChangeDate"] = lastChangeDate
		networkRes = self.ns.decode( self.ns.request("/dbtransfer/iterValues", reqDict) )
		while True:
			if len( networkRes["values"] )==0:
				raise StopIteration()
			for res in networkRes["values"]:
				yield res
			reqDict["cursor"] = networkRes["cursor"]
			networkRes = self.ns.decode( self.ns.request("/dbtransfer/iterValues", reqDict) )

	def getEntry(self, modul, key):
		reqDict = {"module":modul, "id": key, "key":self.exportKey }

		r = self.ns.decode(self.ns.request("/dbtransfer/getEntry", reqDict))
		if not r or not r["object"]:
			return None

		return r["object"]
