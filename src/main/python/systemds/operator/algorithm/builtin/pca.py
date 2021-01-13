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
 
def pca(X: OperationNode, **kwargs: Dict[str, VALID_INPUT_TYPES]) -> OperationNode:
    """
    :param X: Input feature matrix
    :param K: Number of reduced dimensions (i.e., columns)
    :param Center: Indicates whether or not to center the feature matrix
    :param Scale: Indicates whether or not to scale the feature matrix
    :return: 'OperationNode' containing output dominant eigen vectors (can be used for projections) & the column means of the input, subtracted to construct the pca & the scaling of the values, to make each dimension same size. 
    """
    
    X._check_matrix_op()
    if X.shape[0] == 0:
        raise ValueError("Found array with 0 feature(s) (shape={s}) while a minimum of 1 is required."
                         .format(s=X.shape))
    params_dict = {'X':X}
    params_dict.update(kwargs)
    return OperationNode(X.sds_context, 'pca', named_input_nodes=params_dict, output_type=OutputType.LIST, number_of_outputs=4, output_types=[OutputType.MATRIX, OutputType.MATRIX, OutputType.MATRIX, OutputType.MATRIX])


    