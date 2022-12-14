#!/usr/bin/env python3
# coding: utf-8
import unittest
import testgres
import time
import os
import re
import subprocess
import shutil
import glob

from .base_test import BaseTest

class BranchingTest(BaseTest):
	def test_branching_offline(self):
		with self.node as master:
			master.start()
			master.execute("CREATE EXTENSION orioledb;")
			master.execute("CREATE TABLE o_test\n"
							"    (id integer NOT NULL PRIMARY KEY,\n"
							"     value integer NOT NULL)\n"
							"USING orioledb;")
			master.execute("INSERT INTO o_test (SELECT id, id + 1 FROM generate_series(1, 100000) id);")
			master.execute("CHECKPOINT;")
			master.stop()

			branch = self.getBranch()
			branch.start()
			self.assertEqual(branch.execute("SELECT COUNT(*) FROM o_test;")[0][0], 100000)
			self.assertEqual(branch.execute("SELECT SUM(value) FROM o_test;")[0][0], 5000150000)
			branch.execute("UPDATE o_test SET value = value + 10 WHERE id BETWEEN 1 AND 100;")
			branch.stop()

			branch.start()
			self.assertEqual(branch.execute("SELECT COUNT(*) FROM o_test;")[0][0], 100000)
			self.assertEqual(branch.execute("SELECT SUM(value) FROM o_test;")[0][0], 5000151000)
			branch.stop()

	def test_branching_online(self):
		with self.node as master:
			master.start()
			master.execute("CREATE EXTENSION orioledb;")
			master.execute("CREATE TABLE o_test\n"
							"    (id integer NOT NULL PRIMARY KEY,\n"
							"     value integer NOT NULL)\n"
							"USING orioledb;")
			master.execute("INSERT INTO o_test (SELECT id, id + 1 FROM generate_series(1, 100000) id);")
			master.execute("CHECKPOINT;")
			master.stop()

			branch = self.getBranch()
			branch.start()
			self.assertEqual(branch.execute("SELECT COUNT(*) FROM o_test;")[0][0], 100000)
			self.assertEqual(branch.execute("SELECT SUM(value) FROM o_test;")[0][0], 5000150000)
			branch.execute("UPDATE o_test SET value = value + 10 WHERE id BETWEEN 1 AND 100;")
			branch.stop()

			branch.start()
			self.assertEqual(branch.execute("SELECT COUNT(*) FROM o_test;")[0][0], 100000)
			self.assertEqual(branch.execute("SELECT SUM(value) FROM o_test;")[0][0], 5000151000)
			branch.stop()
