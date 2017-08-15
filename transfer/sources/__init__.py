# -*- coding: utf-8 -*-

import os, logging


# Auto import plugins
for _module in os.listdir(os.path.dirname(__file__)):

	if _module == "__init__.py" or not _module.endswith(".py"):
		continue

	_module = _module[:-3]

	try:
		_import = __import__(_module, globals(), locals(), [_module])
		for _name in dir(_import):
			if _name.startswith("_"):
				continue

			_symbol = getattr(_import, _name)
			try:
				if issubclass(_symbol, Plugin):
					globals().update({_name: _symbol})
			except TypeError:  # We might see imports of other modules here (where issubclass failes)
				pass
	except:
		logging.error("Unable to import '%s'" % _module)
		raise
