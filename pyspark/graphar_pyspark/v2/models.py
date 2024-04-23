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

"""Base models for GraphAr."""

from __future__ import annotations

from enum import Enum
from typing import Optional, Self

from pydantic import BaseModel
from pyspark.sql import SparkSession

from .helpers import read_yaml_from_any, write_yaml_to_any


class GarType(str, Enum):
    """Data types for Gar."""

    BOOl = "bool"
    INT32 = "int32"
    INT64 = "int64"
    FLOAT = "float"
    DOUBLE = "double"
    STRING = "string"
    LIST = "list"


class FileType(str, Enum):
    """Supported file types."""

    CSV = "csv"
    PARQUET = "parquet"
    ORC = "orc"


class AdjListType(str, Enum):
    """Supported adjacent list types."""

    UNORDERED_BY_SOURCE = "unordered_by_source"
    UNORDERED_BY_DEST = "unordered_by_dest"
    ORDERED_BY_SOURCE = "ordered_by_source"
    ORDERED_BY_DEST = "ordered_by_dest"


class Property(BaseModel):
    """Property model."""

    name: str = ""
    data_type: GarType = ""
    is_primary: bool = False
    is_nullable: Optional[bool] = None


class PropertyGroup(BaseModel):
    """Property group."""

    prefix: str = ""
    file_type: FileType = FileType.PARQUET
    properties: list[Property] = []


class AdjList(BaseModel):
    """Adjacent list model."""

    ordered: bool = False
    aligned_by: AdjListType = AdjListType.UNORDERED_BY_SOURCE
    prefix: str = ""
    file_type: FileType = FileType.PARQUET


class YAMLSerializable(BaseModel):
    """Base class for YAML-serializable models."""

    @classmethod
    def from_yaml(cls: type[Self], file_path: str, spark: SparkSession) -> Self:
        """Load from YAML file.

        :param file_path: Path to YAML file.
        :param spark: SparkSession instance.
        :return: Instance of the class
        """
        return read_yaml_from_any(file_path, cls, spark)

    def to_yaml(self: Self, file_path: str, spark: SparkSession) -> None:
        """Serialize to YAML file.

        :param file_path: Path to YAML file.
        :param spark: SparkSession instance.
        :return: None
        """
        write_yaml_to_any(file_path, self, spark)


class VertexInfo(YAMLSerializable):
    """Vertex info model."""

    label: str = ""
    chunk_size: int = 0
    prefix: str = ""
    property_groups: list[PropertyGroup] = []
    version: str = ""


class EdgeInfo(YAMLSerializable):
    """Edge info model."""

    src_label: str = ""
    edge_label: str = ""
    dst_label: str = ""
    chunk_size: int = 0
    src_chunk_size: int = 0
    dst_chunk_size: int = 0
    directed: bool = False
    prefix: str = ""
    adj_lists: list[AdjList] = []
    property_groups: list[PropertyGroup] = []
    version: str = ""


class GraphInfo(YAMLSerializable):
    """Graph info model."""

    name: str = ""
    prefix: str = ""
    vertices: list[str] = []
    edges: list[str] = []
    version: str = ""
