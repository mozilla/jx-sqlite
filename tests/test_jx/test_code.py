'''
Don't mind this file.
Created to understand the codebase
'''


from __future__ import division
from __future__ import unicode_literals

from tests.test_jx import BaseTestCase, TEST_TABLE, global_settings, NULL



class TestCode(BaseTestCase):

    def test_a(self):
        test = {
           "data": [{"a": 1}],
           "query": {
               "select": "*",
               "from": TEST_TABLE
           },
           "expecting_list": {
               "meta": {"format": "list"}, "data": [{"a": 1}]
           }
       }
        self.utils.execute_es_tests(test)

