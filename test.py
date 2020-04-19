import unittest
import json
import sys, os.path

path_dir = (os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append (path_dir)

from appv1 import *

class TestAppV1(unittest.TestCase): 

    def setUp (self):
        app.testing = True
        self.app = app.test_client ()

    def test_Hello_World (self):
        response = self.app.get ('/')
        self.assertEqual (response.status_code, 200)
        self.assertIn (b'Hello, World! ', response.data) # b pass string to bytes

    def test_v1_24 (self):
        response = self.app.get ('/ service / v1 / prediction / 24 hours /')
        self.assertEqual (response.status_code, 200)

    def test_v1_48 (self):
        response = self.app.get ('/ service / v1 / prediction / 48 hours /')
        self.assertEqual (response.status_code, 200)

    def test_v1_72 (self):
        response = self.app.get ('/ service / v1 / prediction / 72 hours /')
        self.assertEqual (response.status_code, 200)
