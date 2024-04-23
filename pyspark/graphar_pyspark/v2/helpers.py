# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Helper functions."""
from __future__ import annotations

import yaml
from pydantic.generics import GenericModel
from pyspark.sql import SparkSession


def read_yaml_from_any(location: str, model: type[GenericModel], spark: SparkSession) -> GenericModel:
    """Read YAML model from a file.

    :param location: location of the YAML file
    :param model: model to read
    :param spark: spark session
    :return: validated model
    """
    spark_text = spark.read.text(location)
    raw_text = "\n".join(row["value"] for row in spark_text.collect())
    pydict = yaml.safe_load(raw_text)
    return model.validate(pydict)


def write_yaml_to_any(location: str, model: GenericModel, spark: SparkSession) -> None:
    """Write model to a YAML file.

    :param location: location of the output YAML file
    :param model: model to write
    :param spark: spark session
    :return: None
    """
    pydict = model.model_dump(mode="python")
    raw_text = [tuple(v) for v in yaml.safe_dump(pydict).split("\n")]
    spark_df = spark.createDataFrame(raw_text)
    spark_df.write.format("text").mode("overwrite").option("header", "false").save(location)
