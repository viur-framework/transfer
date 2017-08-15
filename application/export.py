# Mausbrand DB-Backup Appliance
# (C) 2014-17 by Mausbrand Informationssysteme GmbH

from google.appengine.ext import webapp
from google.appengine.ext import blobstore
from google.appengine.api.images import get_serving_url
from google.appengine.ext.blobstore import BlobInfo
from google.appengine.api import datastore, datastore_types
from google.appengine.datastore import datastore_query
import logging, pickle, urlparse, datetime
from itertools import izip
from google.appengine.ext.db import metadata
import string
from key import backupKey


def safeStringComparison(s1, s2):
	"""
		Performs a string comparison in constant time.
		This should prevent side-channel (timing) attacks
		on passwords etc.
		:param s1: First string to compare
		:type s1: string | unicode
		:param s2: Second string to compare
		:type s2: string | unicode
		:return: True if both strings are equal, False otherwise
		:return type: bool
	"""
	isOkay = True
	if type(s1) != type(s2):
		isOkay = False  # We have a unicode/str messup here
	if len(s1) != len(s2):
		isOkay = False
	for x, y in izip(s1, s2):
		if x != y:
			isOkay = False
	return isOkay


class MainPage(webapp.RequestHandler):
	def get(self, path="/", *args, **kwargs):
		self.response.headers['Content-Type'] = 'text/plain'
		# Prevent Hash-collision attacks
		assert len(self.request.arguments()) < 50
		# Fill the (surprisingly empty) kwargs dict with named request params
		tmpArgs = dict((k, self.request.get_all(k)) for k in self.request.arguments())
		for key in tmpArgs.keys()[:]:
			if len(tmpArgs[key]) == 0:
				continue
			if not key in kwargs.keys():
				if len(tmpArgs[key]) == 1:
					kwargs[key] = tmpArgs[key][0]
				else:
					kwargs[key] = tmpArgs[key]
			else:
				if isinstance(kwargs[key], list):
					kwargs[key] = kwargs[key] + tmpArgs[key]
				else:
					kwargs[key] = [kwargs[key]] + tmpArgs[key]
		del tmpArgs
		if "self" in kwargs.keys():  # self is reserved for bound methods
			raise NotImplementedError()
		path = urlparse.urlparse(path).path
		pathlist = [urlparse.unquote(x) for x in path.strip("/").split("/")]
		if len(pathlist) < 2:
			raise NotImplementedError()
		tfunc = pathlist[1]
		pathlist = pathlist[2:]
		if tfunc == "exportDb":
			self.response.write(self.exportDb(*pathlist, **kwargs))
		elif tfunc == "exportBlob":
			self.response.write(self.exportBlob(*pathlist, **kwargs))
		elif tfunc == "download":
			self.response.write(self.download(*pathlist, **kwargs))
		elif tfunc == "info":
			self.response.write(self.info(*pathlist, **kwargs))
		elif tfunc == "listCursors":
			self.response.write(self.listCursors(*pathlist, **kwargs))
		elif tfunc == "listKinds":
			self.response.write(self.listKinds(*pathlist, **kwargs))
		elif tfunc == "_ah":
			pass
		else:
			raise NotImplementedError()

	def post(self, *args, **kwargs):
		return (self.get(*args, **kwargs))

	def info(self, backupkey=None, *args, **kwargs):
		global backupKey
		assert safeStringComparison(backupKey, backupkey)
		return pickle.dumps({"version": 2,
		                     "features": ["async", "selectkinds"]}).encode("HEX")

	def listCursors(self, backupkey=None, cursor=None, kind=None, *args, **kwargs):
		assert safeStringComparison(backupKey, backupkey)
		if cursor:
			c = datastore_query.Cursor(urlsafe=cursor)
		else:
			c = None
		r = []
		for x in range(0,10):
			q = datastore.Query(kind, cursor=c)
			q.Get(1, offset=999)
			newCursor = q.GetCursor()
			if newCursor != c:
				c = newCursor
				r.append(c.urlsafe())
			else:
				break
		return (pickle.dumps({"cursors": r}).encode("HEX"))

	def listKinds(self, backupkey=None, *args, **kwargs):
		global backupKey
		assert safeStringComparison(backupKey, backupkey)
		return (pickle.dumps({"kinds": metadata.get_kinds()}).encode("HEX"))

	def exportDb(self, cursor=None, backupkey=None, endcursor=None, kind=None, *args, **kwargs):
		global backupKey
		assert safeStringComparison(backupKey, backupkey)
		if cursor:
			c = datastore_query.Cursor(urlsafe=cursor)
		else:
			c = None
		if endcursor:
			endCursor = datastore_query.Cursor(urlsafe=endcursor)
		else:
			endCursor = None
		q = datastore.Query(kind, cursor=c, end_cursor=endCursor)
		logging.error((cursor, backupkey, endcursor, kind))
		r = []
		for res in q.Run(limit=5):
			r.append(self.genDict(res))
		return (pickle.dumps({"cursor": str(q.GetCursor().urlsafe()), "values": r}).encode("HEX"))

	exportDb.exposed = True

	def exportBlob(self, cursor=None, backupkey=None, ):
		global backupKey
		assert safeStringComparison(backupKey, backupkey)
		q = BlobInfo.all()
		if cursor is not None:
			q.with_cursor(cursor)
		r = []
		for res in q.run(limit=5):
			r.append(str(res.key()))
		return (pickle.dumps({"cursor": str(q.cursor()), "values": r}).encode("HEX"))

	exportBlob.exposed = True

	def download(self, blobKey, fileName="", download="", *args, **kwargs):
		if download == "1":
			fname = "".join([c for c in fileName if
			                 c in string.ascii_lowercase + string.ascii_uppercase + string.digits + ".-_"])
			self.response.headers.add_header("Content-disposition",
			                                 ("attachment; filename=%s" % (fname)).encode("UTF-8"))
		info = blobstore.get(blobKey)
		if not info:
			raise NotImplementedError()
		self.response.clear()
		self.response.headers['Content-Type'] = str(info.content_type)
		self.response.headers[blobstore.BLOB_KEY_HEADER] = str(blobKey)
		return ("")

	def genDict(self, obj):
		res = {}
		for k, v in obj.items():
			if not any([isinstance(v, x) for x in
			            [str, unicode, long, float, datetime.datetime, list, dict, bool, type(None)]]):
				logging.error("UNKNOWN TYPE %s" % str(type(v)))
				v = unicode(v)
				logging.error(v)
			if isinstance(v, datastore_types.Text):
				v = unicode(v)
			elif isinstance(v, datastore_types.Blob):
				continue
			elif isinstance(v, datastore_types.BlobKey):
				continue
			elif isinstance(v, datastore_types.ByteString):
				v = str(v)
			elif isinstance(v, datastore_types.Category):
				v = unicode(v)
			elif isinstance(v, datastore_types.Email):
				v = unicode(v)
			elif isinstance(v, datastore_types.EmbeddedEntity):
				continue
			elif isinstance(v, datastore_types.GeoPt):
				continue
			elif isinstance(v, datastore_types.IM):
				continue
			elif isinstance(v, datastore_types.Link):
				v = unicode(v)
			elif isinstance(v, datastore_types.PhoneNumber):
				v = unicode(v)
			elif isinstance(v, datastore_types.PostalAddress):
				v = unicode(v)
			elif isinstance(v, datastore_types.Rating):
				v = long(v)
			if "datastore" in str(type(v)):
				logging.error(str(type(v)))
			res[k] = v
		res["id"] = str(obj.key())
		return (res)


application = webapp.WSGIApplication([(r'/(.*)', MainPage), ], debug=True)

