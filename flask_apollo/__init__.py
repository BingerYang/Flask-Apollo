# -*- coding: utf-8 -*-
# @Time     : 2021/6/7 上午11:23
# @Author   : binger
name = __package__
version_info = (0, 2, 24010420)
__version__ = ".".join([str(v) for v in version_info])
__description__ = 'Flask Apollo Apply'

from ._apollo_app import FlaskApollo
