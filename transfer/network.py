#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from queue import Queue
import urllib, urllib.request, urllib.error, urllib.parse
from urllib.parse import quote_plus
import sys, os, os.path, time
import json
import mimetypes
import ssl
import string,  random
from threading import local, Lock
import http.cookiejar
import base64
from queue import Queue, Empty as QEmpty, Full as QFull
from hashlib import sha1
from PyQt4 import QtCore, QtGui
from PyQt4.QtCore import QUrl, QObject
from PyQt4.QtNetwork import QNetworkAccessManager, QNetworkRequest, QSslConfiguration, QSslCertificate, QNetworkReply
import traceback
import logging
import weakref

##Setup the SSL-Configuration. We accept only the two known Certificates from google; reject all other
try:
	certs = open("cacert.pem", "r").read()
except:
	certs = None
if certs:
	baseSslConfig = QSslConfiguration.defaultConfiguration()
	baseSslConfig.setCaCertificates( QSslCertificate.fromData( certs ) )
	QSslConfiguration.setDefaultConfiguration( baseSslConfig )
	nam = QNetworkAccessManager()
	_isSecureSSL = True
else:
	#We got no valid certificate file - accept all SSL connections
	nam = QNetworkAccessManager()
	class SSLFIX( QtCore.QObject ):
		def onSSLError(self, networkReply,  sslErros ):
			networkReply.ignoreSslErrors()
	_SSLFIX = SSLFIX()
	nam.sslErrors.connect( _SSLFIX.onSSLError )
	_isSecureSSL = False

mimetypes.init()
if os.path.exists("mime.types"):
	mimetypes.types_map.update( mimetypes.read_mime_types("mime.types") )

#Source: http://srinikom.github.io/pyside-docs/PySide/QtNetwork/QNetworkReply.html
NetworkErrorDescrs = {
	"ConnectionRefusedError": "The remote server refused the connection (the server is not accepting requests)",
	"RemoteHostClosedError": "The remote server closed the connection prematurely, before the entire reply was received and processed",
	"HostNotFoundError": "The remote host name was not found (invalid hostname)",
	"TimeoutError": "The connection to the remote server timed out",
	"OperationCanceledError": "The operation was canceled via calls to PySide.QtNetwork.abort() or PySide.QtNetwork.close() before it was finished.",
	"SslHandshakeFailedError": "The SSL/TLS handshake failed and the encrypted channel could not be established. The PySide.QtNetwork.sslErrors() signal should have been emitted.",
	"TemporaryNetworkFailureError": "The connection was broken due to disconnection from the network, however the system has initiated roaming to another access point. The request should be resubmitted and will be processed as soon as the connection is re-established.",
	"ProxyConnectionRefusedError": "The connection to the proxy server was refused (the proxy server is not accepting requests)",
	"ProxyConnectionClosedError": "The proxy server closed the connection prematurely, before the entire reply was received and processed",
	"ProxyNotFoundError": "The proxy host name was not found (invalid proxy hostname)",
	"ProxyTimeoutError": "The connection to the proxy timed out or the proxy did not reply in time to the request sent",
	"ProxyAuthenticationRequiredError": "The proxy requires authentication in order to honour the request but did not accept any credentials offered (if any)",
	"ContentAccessDenied": "The access to the remote content was denied (similar to HTTP error 401)",
	"ContentOperationNotPermittedError": "The operation requested on the remote content is not permitted",
	"ContentNotFoundError": "The remote content was not found at the server (similar to HTTP error 404)",
	"AuthenticationRequiredError": "The remote server requires authentication to serve the content but the credentials provided were not accepted (if any)",
	"ContentReSendError": "The request needed to be sent again, but this failed for example because the upload data could not be read a second time.",
	"ProtocolUnknownError": "The Network Access API cannot honor the request because the protocol is not known",
	"ProtocolInvalidOperationError": "The requested operation is invalid for this protocol",
	"UnknownNetworkError": "An unknown network-related error was detected",
	"UnknownProxyError": "An unknown proxy-related error was detected",
	"UnknownContentError": "An unknown error related to the remote content was detected",
	"ProtocolFailure": "A breakdown in protocol was detected (parsing error, invalid or unexpected responses, etc.)"
}
#Match keys of that array with the numeric values suppied by QT
for k,v in NetworkErrorDescrs.copy().items():
	try:
		NetworkErrorDescrs[ getattr( QNetworkReply, k ) ] = v
	except:
		pass #Some errors don't seem to exist on all Platforms (eg. TemporaryNetworkFailureError seems missing on MacOs)
	del NetworkErrorDescrs[ k ]

class SecurityTokenProvider( QObject ):
	"""
		Provides an pool of valid securitykeys.
		As they dont have to be requested before the original request can be send,
		the whole process speeds up
	"""
	errorCount = 0
	
	def __init__(self, *args, **kwargs ):
		super( SecurityTokenProvider, self ).__init__( *args, **kwargs )
		self.logger = logging.getLogger( "RequestWrapper" )
		self.queue = Queue( 5 ) #Queue of valid tokens
		self.isRequesting = False
	
	def reset(self):
		"""
			Flushes the cache and tries to rebuild it
		"""
		self.logger.debug("Reset" )
		while not self.queue.empty():
			self.queue.get( False )
		self.isRequesting = False
	
	def fetchNext( self ):
		"""
			Requests a new SKey if theres currently no request pending
		"""
		if not self.isRequesting:
			if SecurityTokenProvider.errorCount>5: #We got 5 Errors in a row
				raise RuntimeError("Error-limit exceeded on fetching skey")
			self.logger.debug( "Fetching new skey" )
			self.isRequesting = True
			NetworkService.request("/skey", successHandler=self.onSkeyAvailable, failureHandler=self.onError )
	
	def onError(self, request, error ):
		SecurityTokenProvider.errorCount += 1
		self.logger.warning( "Error fetching skey: %s", str(error) )
		self.isRequesting = False
	
	def onSkeyAvailable(self, request=None ):
		"""
			New SKey got avaiable
		"""
		self.isRequesting = False
		try:
			skey = NetworkService.decode( request )
		except:
			SecurityTokenProvider.errorCount += 1
			self.isRequesting = False
			return
		if SecurityTokenProvider.errorCount>0:
			SecurityTokenProvider.errorCount = 0
		self.isRequesting = False
		if not skey:
			return
		try:
			self.queue.put( (skey,time.time()), False )
		except QFull:
			print( "Err: Queue FULL" )
	
	def getKey(self):
		"""
			Returns a fresh, valid SKey from the pool.
			Blocks and requests a new one if the Pool is currently empty.
		"""
		self.logger.debug( "Consuming a new skey" )
		skey = None
		while not skey:
			self.fetchNext()
			try:
				skey, creationTime = self.queue.get( False )
				if creationTime<time.time()-600: #Its older than 10 minutes - dont use
					self.logger.debug( "Discarding old skey" )
					skey = None
					raise QEmpty()
			except QEmpty:
				self.logger.debug( "Empty cache! Please wait..." )
				QtCore.QCoreApplication.processEvents()
		self.logger.debug( "Using skey: %s", skey )
		return( skey )
	
securityTokenProvider = SecurityTokenProvider()

class RequestWrapper( QtCore.QObject ):
	GarbargeTypeName = "RequestWrapper"
	requestSucceeded = QtCore.pyqtSignal( (QtCore.QObject,) )
	requestFailed = QtCore.pyqtSignal( (QtCore.QObject, QNetworkReply.NetworkError) )
	finished = QtCore.pyqtSignal( (QtCore.QObject,) )
	uploadProgress = QtCore.pyqtSignal( (QtCore.QObject,int,int) )
	downloadProgress = QtCore.pyqtSignal( (QtCore.QObject,int,int) )

	def __init__(self, request, successHandler=None, failureHandler=None, finishedHandler=None, parent=None, url=None ):
		super( RequestWrapper, self ).__init__()
		self.logger = logging.getLogger( "RequestWrapper" )
		self.logger.debug("New network request: %s", str(self) )
		self.request = request
		self.url = url
		request.setParent( self )
		self.hasFinished = False
		request.downloadProgress.connect( self.onDownloadProgress )
		request.uploadProgress.connect( self.onUploadProgress )
		request.finished.connect( self.onFinished )

	def onDownloadProgress(self, bytesReceived, bytesTotal ):
		if bytesReceived == bytesTotal:
			self.requestStatus = True
		self.downloadProgress.emit( self, bytesReceived, bytesTotal )
	
	
	def onUploadProgress(self, bytesSend, bytesTotal ):
		if bytesSend == bytesTotal:
			self.requestStatus = True
		self.uploadProgress.emit( self, bytesSend, bytesTotal )

	def onFinished(self ):

		self.hasFinished = True
		if self.request.error()==self.request.NoError:
			self.requestSucceeded.emit( self )
		else:
			try:
				errorDescr = NetworkErrorDescrs[ self.request.error() ]
			except: #Unknown error 
				errorDescr = None
			if errorDescr:
				QtGui.QMessageBox.warning( None, "Networkrequest Failed", "The request to \"%s\" failed with: %s" % (self.url, errorDescr) )
			self.requestFailed.emit( self, self.request.error() )
		self.finished.emit( self )
		self.logger.debug("Request finished: %s", str(self) )
		self.logger.debug("Remaining requests: %s",  len(NetworkService.currentRequests) )


	def readAll(self):
		return( self.request.readAll() )
	
	def abort( self ):
		self.request.abort()

	def getResult(self):
		while not self.hasFinished:
			QtCore.QCoreApplication.processEvents()
		return( json.loads( self.request.readAll().data().decode("UTF-8") ) )





class NetworkService():
	url = None
	currentRequests = [] #A list of currently running requests
	
	@staticmethod
	def genReqStr( params ):
		boundary_str = "---"+''.join( [ random.choice(string.ascii_lowercase+string.ascii_uppercase + string.digits) for x in range(13) ] ) 
		boundary = boundary_str.encode("UTF-8")
		res = b'Content-Type: multipart/mixed; boundary="'+boundary+b'"\r\nMIME-Version: 1.0\r\n'
		res += b'\r\n--'+boundary
		for(key, value) in list(params.items()):
			if all( [x in dir( value ) for x in ["name", "read"] ] ): #File
				try:
					(type, encoding) = mimetypes.guess_type( value.name.decode( sys.getfilesystemencoding() ), strict=False )
					type = type or "application/octet-stream"
				except:
					type = "application/octet-stream"
				res += b'\r\nContent-Type: '+type.encode("UTF-8")+b'\r\nMIME-Version: 1.0\r\nContent-Disposition: form-data; name="'+key.encode("UTF-8")+b'"; filename="'+os.path.basename(value.name).decode(sys.getfilesystemencoding()).encode("UTF-8")+b'"\r\n\r\n'
				res += value.read()
				res += b'\r\n--'+boundary
			elif isinstance( value, list ):
				for val in value:
					res += b'\r\nContent-Type: application/octet-stream\r\nMIME-Version: 1.0\r\nContent-Disposition: form-data; name="'+key.encode("UTF-8")+b'"\r\n\r\n'
					res += str( val ).encode("UTF-8")
					res += b'\r\n--'+boundary
			else:
				res += b'\r\nContent-Type: application/octet-stream\r\nMIME-Version: 1.0\r\nContent-Disposition: form-data; name="'+key.encode("UTF-8")+b'"\r\n\r\n'
				res += str( value ).encode("UTF-8")
				res += b'\r\n--'+boundary
		res += b'--\r\n'
		return( res, boundary )

	@staticmethod
	def request( url, params=None, secure=False, extraHeaders=None, successHandler=None, failureHandler=None, finishedHandler=None, parent=None ):
		global nam, _isSecureSSL
		if _isSecureSSL==False and False: #Warn the user of a potential security risk
			msgRes = QtGui.QMessageBox.warning(	None, QtCore.QCoreApplication.translate("NetworkService", "Insecure connection"),
								QtCore.QCoreApplication.translate("Updater", "The cacerts.pem file is missing or invalid. Your passwords and data will be send unsecured! Continue without encryption? If unsure, choose \"abort\"!"), 
								QtCore.QCoreApplication.translate("NetworkService", "Continue in unsecure mode"),
								QtCore.QCoreApplication.translate("NetworkService", "Abort") )
			if msgRes==0:
				_isSecureSSL=None
			else:
				sys.exit(1)
		if secure:
			key=securityTokenProvider.getKey()
			if not params:
				params = {}
			params["skey"] = key
		if url.lower().startswith("http"):
			reqURL = QUrl(url)
		else:
			reqURL = QUrl( NetworkService.url+url )
		req = QNetworkRequest( reqURL )
		if extraHeaders:
			for k, v in extraHeaders.items():
				req.setRawHeader( k,  v )
		if params:
			if isinstance( params, dict):
				multipart, boundary = NetworkService.genReqStr( params )
				req.setRawHeader( "Content-Type", b'multipart/form-data; boundary='+boundary+b'; charset=utf-8')
			elif isinstance( params, bytes ):
				req.setRawHeader( "Content-Type",  b'application/x-www-form-urlencoded' )	
				multipart = params
			else:
				print( params )
				print( type( params ) )
			return( RequestWrapper( nam.post( req, multipart ), url=url ).getResult() )
		else:
			return( RequestWrapper( nam.get( req ), url=url).getResult() )
	
	@staticmethod
	def decode( req ):
		return( json.loads( req.readAll().data().decode("UTF-8") ) )
	
	@staticmethod
	def setup( url, *args, **kwargs ):
		NetworkService.url = url
		securityTokenProvider.reset()

