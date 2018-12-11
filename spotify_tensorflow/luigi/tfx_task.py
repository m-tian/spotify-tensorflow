# -*- coding: utf-8 -*-
#
# Copyright 2017 Spotify AB.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from abc import abstractmethod

from spotify_tensorflow.luigi.python_dataflow_task import PythonDataflowTask


class TFXBaseTask(PythonDataflowTask):
    def __init__(self, *args, **kwargs):
        super(TFXBaseTask, self).__init__(*args, **kwargs)
        self.job_name = self.__class__.__name__

    def tfx_args(self):
        """ Extra arguments that will be passed to your tfx dataflow job.

        Example:
            return ["--schema_file=gs://uri/to/schema_file"]
        Note that:

            * You "set" args by overriding this method in your tfx subclass.
            * This function should return an iterable of strings.
        """
        return []

    def _mk_cmd_line(self):
        cmd_line = super(TFXBaseTask, self)._mk_cmd_line()
        cmd_line.extend(self.tfx_args())
        return cmd_line


class TFTransformTask(TFXBaseTask):
    def __init__(self, *args, **kwargs):
        super(TFTransformTask, self).__init__(*args, **kwargs)
        self.job_name = self.__class__.__name__

    def tfx_args(self):
        return [
            "--schema_file=%s" % self.get_schema_file()
        ]

    @abstractmethod
    def get_schema_file(self):  # type: () -> str
        """
        Should return fully qualified path to the schema file.
        This has to be defined as a method because the schema file is based on the input dataset
        """
        pass
