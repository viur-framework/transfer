import sqlite3
import pickle
import urllib2
from urllib import quote_plus
from time import time, sleep
import pickle, json, random, mimetypes, string, sys, os
import argparse
from threading import Thread
from Queue import Queue, Empty
from time import sleep
from itertools import chain

parser = argparse.ArgumentParser(description='Download the complete Data- and Blob-Store from the given application.')
parser.add_argument('--appid', type=str, help='Appspot-ID of the Application.', required=True, dest="appid")
parser.add_argument('--key', type=str, help='Download-Key for the Application.', required=True, dest="key")
parser.add_argument('--file', type=str, help='File to backup to.', required=True, dest="file")
parser.add_argument('--parallel', type=int, help='Parallelize Downloads', required=False, dest="parallel", default=0)
parser.add_argument('--partial', help='Partial Download only (exclude redundant data that will be reconstructed on inport anyway). Requires parallel.', required=False, dest="partial", default=False, action="store_true")
parser.add_argument('--list-partial', help='Test if partial download is possible and list covered kinds', required=False, dest="listpartial", default=False, action="store_true")
parser.add_argument('--include-partial', help='List of kinds to explicitly include in partial download', required=False, dest="partialinclude", default=[], action="append", nargs="*")
parser.add_argument('--exclude-partial', help='List of kinds to explicitly exclude in partial download', required=False, dest="partialexclude", default=[], action="append", nargs="*")

args = parser.parse_args()

# Statistics
expectedEntriesMin = 0
expectedEntriesMax = 0
storedEntriesTotal = 0
newBlobsStored = 0
totalBlobsSeen = 0

class NetworkService(object):
	baseURL = None

	def __init__(self, baseURL):
		super(NetworkService, self).__init__()
		self.baseURL = baseURL
		cp = urllib2.HTTPCookieProcessor()
		self.opener = urllib2.build_opener(cp)
		urllib2.install_opener(self.opener)

	@staticmethod
	def genReqStr(params):
		boundary_str = "---" + ''.join(
			[random.choice(string.ascii_lowercase + string.ascii_uppercase + string.digits) for x in
			 range(13)])
		boundary = boundary_str.encode("UTF-8")
		res = b'Content-Type: multipart/mixed; boundary="' + boundary + b'"\nMIME-Version: 1.0\n'
		res += b'\n--' + boundary
		for (key, value) in list(params.items()):
			if all([x in dir(value) for x in ["name", "read"]]):  # File
				try:
					(type, encoding) = mimetypes.guess_type(
						value.name.decode(sys.getfilesystemencoding()), strict=False)
					type = type or b"application/octet-stream"
				except:
					type = b"application/octet-stream"
				res += b'\nContent-Type: ' + type.encode(
					"UTF-8") + b'\nMIME-Version: 1.0\nContent-Disposition: form-data; name="' + key.encode(
					"UTF-8") + b'"; filename="' + os.path.basename(value.name).decode(
					sys.getfilesystemencoding()).encode("UTF-8") + b'"\n\n'
				res += value.read()
				res += b'\n--' + boundary
			elif isinstance(value, list):
				for val in value:
					res += b'\nContent-Type: application/octet-stream\nMIME-Version: 1.0\nContent-Disposition: form-data; name="' + key.encode(
						"UTF-8") + b'"\n\n'
					if isinstance(val, unicode):
						res += val.encode("UTF-8")
					else:
						res += str(val)
					res += b'\n--' + boundary
			else:
				res += b'\nContent-Type: application/octet-stream\nMIME-Version: 1.0\nContent-Disposition: form-data; name="' + key.encode(
					"UTF-8") + b'"\n\n'
				if isinstance(value, unicode):
					res += unicode(value).encode("UTF-8")
				else:
					res += str(value)
				res += b'\n--' + boundary
		res += b'--\n'
		return (res, boundary)

	def request(self, url, params=None, secure=False, extraHeaders=None, noParse=False, yieldMimeType=False):
		def rWrap(self, url, params=None, secure=False, extraHeaders=None, noParse=False, yieldMimeType=False):
			if secure:
				skey = json.loads(urllib2.urlopen(self.baseURL + "/skey").read())
				if params is None or isinstance(params, bytes):
					if "?" in url:
						url += "&skey=" + skey
					else:
						url += "?skey=" + skey
				else:
					params["skey"] = skey
			if isinstance(params, dict):
				res, boundary = self.genReqStr(params)
				r = urllib2.Request((self.baseURL + url).encode("UTF-8"), res, headers={
					b"Content-Type": b'multipart/form-data; boundary=' + boundary + b'; charset=utf-8'})
				req = urllib2.urlopen(r)
			else:
				req = urllib2.urlopen(self.baseURL + url)
			if noParse:
				if yieldMimeType:
					return (req.read(), req.info().type)
				else:
					return (req.read())
			else:
				return (pickle.loads(req.read().decode("HEX")))

		for x in range(0, 4):
			try:
				return (rWrap(self, url, params, secure, extraHeaders, noParse, yieldMimeType))
			except Exception as e:
				if x < 3:
					print("Error during network request:", e, "Retrying in 60 seconds")
					sleep(60)
				else:
					print("Fatal error in network request, exiting")
					print("Failed request was")
					print(url, params, secure, extraHeaders, noParse)
					raise

def includeKindInPartial(kind):
	# Check if a given kind should be stored
	if kind.lower() in args.partialinclude:
		return True
	elif kind.lower() in args.partialexclude:
		return False
	elif kind.startswith("_") or kind.startswith("viur-") or kind.endswith("_uniquePropertyIndex"):
		return False
	else:
		return True


class AsyncDownloaderThread(Thread):
	"""
		Fetches the list of entries between startCursor - endCursor from remote and yield them through
		outQueue sothat the're stored in sqlite.
	"""

	def start(self, baseUrl, inQueue, outQueue):
		self.baseUrl = baseUrl
		self.inQueue = inQueue
		self.outQueue = outQueue
		self.doContinue = True
		super(AsyncDownloaderThread, self).start()

	def run(self):
		ns = NetworkService(self.baseUrl)
		while self.doContinue:
			numEntities = 0
			try:
				data = self.inQueue.get(block=True, timeout=5)
				(kind, startCursor, endCursor) = data #self.inQueue.get(block=True, timeout=5)
			except Empty:
				continue
			filter = {"cursor": startCursor, "backupkey": backupKey,
			                              "endcursor": endCursor}
			if kind:
				filter["kind"] = kind
			res = ns.request("/export/exportDb", filter)
			while res["values"]:
				if not self.doContinue:
					print("AsyncDownloaderThread stopping while processing a job!")
					return
				numEntities += len(res["values"])
				#print("Got a total of %s entities so far" % numEntities)
				for r in res["values"]:
					self.outQueue.put(r)
				filter["cursor"] = res["cursor"]
				res = ns.request("/export/exportDb", filter)
				if numEntities>1111:
					print("Thread %s fetched %s entries with %s, %s, %s" % (self, numEntities, kind, startCursor, endCursor))


class AsyncCursorThread(Thread):
	"""
		Fetch the lists of intermediate cursors from remote and create corresponding tickets
		in inQueu for the AsyncDownloaderThreads
	"""

	def start(self, baseUrl, inQueue, outQueue):
		self.baseUrl = baseUrl
		self.inQueue = inQueue
		self.outQueue = outQueue
		self.doContinue = True
		super(AsyncCursorThread, self).start()

	def run(self):
		global expectedEntriesMin, expectedEntriesMax, args
		ns = NetworkService(self.baseUrl)
		cursors = []
		numEntities = 0
		isFirst = True
		if args.partial:
			kindNames = ns.request("/export/listKinds", {"backupkey": backupKey,})
			for kind in kindNames["kinds"]:
				isFirst = True
				doBreakInnerLoop = False
				while self.doContinue and not doBreakInnerLoop:
					if not includeKindInPartial(kind):
						print("Ignoring kind %s" % (kind,))
						break
					print("Fetching Cursors for %s" % (kind,))
					res = ns.request("/export/listCursors",
					                 {"backupkey": backupKey,
					                  "cursor": cursors[-1] if cursors else "",
					                  "kind": kind})
					cursors.extend(res["cursors"])
					numEntities += len(res["cursors"])
					#expectedEntriesMax += numEntities * 1000
					if len(res["cursors"]) != 10:
						doBreakInnerLoop = True
					if isFirst and len(cursors):
						inQueue.put((kind, "", cursors[0]))
						isFirst = False
						numEntities += 1
						expectedEntriesMax += 1000
					for x in range(0, len(cursors) - 1):
						inQueue.put((kind, cursors[x], cursors[x + 1]))
						expectedEntriesMin += 1000
						expectedEntriesMax += 1000
					cursors = cursors[-1:]
					print("Got a total of %s cursors so far" % numEntities)
				if cursors:
					inQueue.put((kind, cursors[-1], ""))
					expectedEntriesMax += 1000
				cursors = []
				print("Got all cursors for %s" % kind)
			print("**** Got all cursors! ****")


		else:
			while self.doContinue:
				res = ns.request("/export/listCursors",
				                             {"backupkey": backupKey, "cursor": cursors[-1] if cursors else ""})
				cursors.extend(res["cursors"])
				numEntities += len(res["cursors"])
				expectedEntries = numEntities * 1000
				if len(res["cursors"]) != 10:
					self.doContinue = False
				if isFirst and len(cursors):
					inQueue.put((None, "", cursors[0]))
					isFirst = False
					numEntities += 1
				for x in range(0, len(cursors) - 1):
					inQueue.put((None, cursors[x], cursors[x + 1]))
				cursors = cursors[-1:]
				print("Got a total of %s cursors so far" % numEntities)
			expectedEntries = numEntities * 1000
			inQueue.put((None, cursors[-1], ""))
			print("**** Got all cursors! ****")


def storeEntry(entry):
	# Write an entry to the sqlite database (used only for non-parallel downloads)
	global sqlConn, backupRunID
	cursor = sqlConn.cursor()
	cursor.execute("""insert into entries (id,modul,backuprun,data) VALUES (?,?,?,?)""",
	               (entry["id"], "", backupRunID, pickle.dumps(entry)))
	sqlConn.commit()


def haveBlob(blobKey):
	# Check if we already have the given blobkey in our sqlite database
	global sqlConn, backupRunID
	cursor = sqlConn.cursor()
	r = cursor.execute("""SELECT data FROM blobs WHERE id=?""", (blobKey,))
	return (r.fetchone() is not None)


def fetchBlob(blobKey):
	# Fetch the given blob from remote
	global networkService
	return (networkService.request("/file/download/%s" % blobKey, noParse=True, yieldMimeType=True))


def storeBlob(blobKey, blob, mimetype):
	# Write a new blob-key to the sqlite database
	global sqlConn, backupRunID
	cursor = sqlConn.cursor()
	cursor.execute("""insert or replace into blobs (id,data,mimetype) VALUES (?,?,?)""", (blobKey, blob, mimetype))
	sqlConn.commit()


def fetchRemoteInfo():
	# Try to fetch the version and supported features of the remote site
	global networkService
	try:
		res = networkService.request("/export/info", {"backupkey": backupKey})
	except:
		return None
	return res



# Flatten nested Lists created by argparse
args.partialinclude = [x.lower() for x in (chain(*args.partialinclude))]
args.partialexclude = [x.lower() for x in (chain(*args.partialexclude))]

if args.partial and not args.parallel:
	print("Partial requires parallel for now")
	sys.exit(1)

sqlDbFile = '%s.db' % args.file
viurHost = "https://%s.appspot.com/" % args.appid
# viurHost = "http://127.0.0.1:8080/"
backupKey = args.key

sqlConn = sqlite3.connect(sqlDbFile)
sqlConn.text_factory = str
cursor = sqlConn.cursor()
cursor.execute("""Create Table IF NOT EXISTS entries (id text, modul text, backuprun INTEGER, data blob)""")
cursor.execute("""Create Table IF NOT EXISTS backupruns (id INTEGER primary key autoincrement, startdate float, iscomplete INTEGER)""")
cursor.execute("""Create Table IF NOT EXISTS blobs (id text primary key, data blob ,mimetype text)""")
r = cursor.execute("""insert into backupruns (startdate, iscomplete) VALUES (?,?)""", (time(), 0))
sqlConn.commit()
backupRunID = cursor.lastrowid

networkService = NetworkService(viurHost)

remoteInfo = fetchRemoteInfo()

if args.listpartial:  # Just dump a list of kinds that will be stored and a list of kinds ignored
	coveredKinds = []
	ignoredKinds = []
	if remoteInfo and remoteInfo["version"] > 1 and "async" in remoteInfo["features"]:
		#ns = NetworkService(self.baseUrl)
		kindNames = networkService.request("/export/listKinds", {"backupkey": backupKey,})
		for kind in kindNames["kinds"]:
			if includeKindInPartial(kind):
				coveredKinds.append(kind)
			else:
				ignoredKinds.append(kind)
		coveredKinds.sort()
		ignoredKinds.sort()
		print("Kinds that will be stored: %s" % coveredKinds)
		print("Kinds that will be ignored: %s" % ignoredKinds)
	else:
		print(u"Remote Downloader does not support partial export")
	print( args.partialinclude)
	print(args.partialexclude)
	sys.exit(0)


if args.parallel:
	if not (remoteInfo and remoteInfo["version"] > 1 and "async" in remoteInfo["features"]):
		print("Remote does not support parallel downloads!")
		sys.exit(1)
	cursors = []
	inQueue = Queue()
	outQueue = Queue()
	threadPool = [AsyncDownloaderThread() for x in range(0, args.parallel)]
	#threadPool = []
	asyncCursorThread = AsyncCursorThread()
	threadPool.append(asyncCursorThread)

	for thread in threadPool:
		thread.start(viurHost, inQueue, outQueue)

	localQueue = []
	try:
		#cursor = sqlConn.cursor()
		didTimeout = False
		while(inQueue.qsize()>0 or not didTimeout):
			didTimeout = False
			try:
				entry = outQueue.get(block=True, timeout=30)
			except Empty:
				if not asyncCursorThread.isAlive():
					didTimeout = True
				continue
			localQueue.append((entry["id"], "", backupRunID, pickle.dumps(entry)))
			#cursor.execute("""insert into entries (id,modul,backuprun,data) VALUES (?,?,?,?)""",
			#               (entry["id"], "", backupRunID, pickle.dumps(entry)))
			storedEntriesTotal += 1
			if storedEntriesTotal % 100 == 0:
				print("Stored %s of %s - %s expected entries" % (storedEntriesTotal, expectedEntriesMin, expectedEntriesMax))
				#print("Qsizees: IN %s, OUT %s: " % (inQueue.qsize(), outQueue.qsize()))
				if len(localQueue)>2500:
					cursor.executemany("""insert into entries (id,modul,backuprun,data) VALUES (?,?,?,?)""", localQueue)
					localQueue = []

	except KeyboardInterrupt:
		print("Shutting down!")
		for thread in threadPool:
			thread.doContinue = False
		for thread in threadPool:
			thread.join()
	except Exception as e:
		print("caucht exception")
		print(e)
		print("exiting")
		for thread in threadPool:
			thread.doContinue = False
		for thread in threadPool:
			thread.join()
		raise

	cursor.executemany("""insert into entries (id,modul,backuprun,data) VALUES (?,?,?,?)""", localQueue)
	localQueue = []
	sqlConn.commit()

	sleep(5)
	for thread in threadPool:
		thread.doContinue = False
	for thread in threadPool:
		thread.join()
	print("Threads down")
	print( outQueue.qsize())
	if storedEntriesTotal < expectedEntriesMin:
		print("****** Fetched too few items: %s; expected at least %s ******" % (storedEntriesTotal, expectedEntriesMin))
	elif storedEntriesTotal > expectedEntriesMax:
		print("****** Fetched too much items: %s; expected at most %s  ******" % (storedEntriesTotal,expectedEntriesMax))
	else:
		print("****** Fetched %s items; that's within the expected range of %s - %s  ******" % (storedEntriesTotal, expectedEntriesMin, expectedEntriesMax))
else:
	# Fetch entities
	res = networkService.request("/export/exportDb", {"backupkey": backupKey})
	while res["values"]:
		storedEntriesTotal += len(res["values"])
		print("Got a total of %s entities so far" % storedEntriesTotal)
		for r in res["values"]:
			storeEntry(r)
		res = networkService.request("/export/exportDb", {"cursor": res["cursor"], "backupkey": backupKey})


# Fetch blobs
res = networkService.request("/export/exportBlob", {"backupkey": backupKey})
numBlobs = 0
while res["values"]:
	numBlobs += len(res["values"])
	print("Got a total of %s blobs so far" % numBlobs)
	for r in res["values"]:
		totalBlobsSeen += 1
		if not haveBlob(r):
			print("Fetching new blobkey %s" % r)
			storeBlob(r, *fetchBlob(r))
			newBlobsStored += 1
	res = networkService.request("/export/exportBlob", {"cursor": res["cursor"], "backupkey": backupKey})

# Mark the current run as complete
r = cursor.execute("""update backupruns SET iscomplete=? WHERE id=?""", (1, backupRunID))
sqlConn.commit()

print(u"Backup of %s complete" % args.appid)
print(u"Got %s entries and %s Blobs (%s of them where seen for the first time)" % (storedEntriesTotal, totalBlobsSeen, newBlobsStored))

