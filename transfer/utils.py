# -*- coding: utf-8 -*-

from gkey.key import Key

class StopPropagationException( Exception ):
	"""
		Stops an action from sinking/bubbling further
	"""
	pass


def rewriteKey(keyIn, appID):
	if not keyIn:
		return None
	if isinstance(keyIn, basestring):
		keyIn = Key(encoded=keyIn)
	return Key.from_path(keyIn.kind(), keyIn.id_or_name(), parent=rewriteKey(keyIn.parent(), appID), _app=appID)
