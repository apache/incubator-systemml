# -------------------------------------------------------------
#
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
#
# -------------------------------------------------------------

from typing import Dict

from systemds.operator import OperationNode
from systemds.script_building.dag import OutputType
from systemds.utils.consts import VALID_INPUT_TYPES
 
def kmeans(X: OperationNode, **kwargs: Dict[str, VALID_INPUT_TYPES]) -> OperationNode:
    """
    :param X: The input Matrix to do KMeans on.
    :param k: Number of centroids
    :param runs: Number of runs (with different initial centroids)
    :param max_iter: Maximum number of iterations per run
    :param eps: Tolerance (epsilon) for WCSS change ratio
    :param is_verbose: do not print per-iteration stats
    :param avg_sample_size_per_centroid: Average number of records per centroid in data samples
    :param seed: The seed used for initial sampling. If set to -1 random seeds are selected.
    :return: 'OperationNode' containing the mapping of records to centroids & the output matrix with the centroids 
    """
    
    X._check_matrix_op()
    if X.shape[0] == 0:
        raise ValueError("Found array with 0 feature(s) (shape={s}) while a minimum of 1 is required."
                         .format(s=X.shape))
    params_dict = {'X':X}
    params_dict.update(kwargs)
    return OperationNode(X.sds_context, 'kmeans', named_input_nodes=params_dict, output_type=OutputType.LIST, number_of_outputs=2, output_types=[OutputType.MATRIX, OutputType.MATRIX])


    