'''
udata datalake service
'''

import os

tag = os.environ.get('CIRCLE_TAG')
build_num = os.environ.get('CIRCLE_BUILD_NUM')

__version__ = '0.0.0' + ('.dev' + str(build_num) if not tag and build_num else '')
