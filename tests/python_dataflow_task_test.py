# -*- coding: utf-8 -*-
#
#  Copyright 2017 Spotify AB.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from __future__ import absolute_import, division, print_function

from unittest import TestCase

import luigi
from luigi.contrib.gcs import GCSTarget
from spotify_tensorflow.luigi.python_dataflow_task import PythonDataflowTask


class DummyRawFeature(luigi.ExternalTask):
    def output(self):
        return GCSTarget("output_uri")


class DummyTFXTask(PythonDataflowTask):
    python_script = "pybeamjob.py"
    requirements_file = "tfx_requirement.txt"

    def tfx_args(self):
        return ["--schema_file=schema.pb"]


class DummyUserTfxTask(DummyTFXTask):
    project = "dummy"
    worker_machine_type = "n1-standard-4"
    num_workers = 5
    max_num_workers = 20
    autoscaling_algorithm = "THROUGHPUT_BASED"
    service_account = "dummy@dummy.iam.gserviceaccount.com"
    local_runner = True
    staging_location = "staging_uri"

    def requires(self):
        return {"input": DummyRawFeature()}

    def args(self):
        return ["--foo=bar"]

    def output(self):
        return GCSTarget(path="output_uri")


class PythonDataflowTaskTest(TestCase):

    def test_task(self):
        task = DummyUserTfxTask()

        expected = [
            "python",
            "pybeamjob.py",
            "--runner=DirectRunner",
            "--project=dummy",
            "--autoscaling_algorithm=THROUGHPUT_BASED",
            "--num_workers=5",
            "--max_num_workers=20",
            "--service_account_email=dummy@dummy.iam.gserviceaccount.com",
            "--input=output_uri/part-*",
            "--output=output_uri",
            "--staging_location=staging_uri",
            "--requirements_file=tfx_requirement.txt",
            "--worker_machine_type=n1-standard-4",
            "--schema_file=schema.pb",
            "--foo=bar"
        ]
        expected.sort()
        actual = task._mk_cmd_line()
        actual.sort()
        self.assertEquals(actual, expected)
