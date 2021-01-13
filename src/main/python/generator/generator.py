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

from typing import Tuple, List
import json
import os
import re
from parser import FunctionParser


class PythonAPIFileGenerator(object):

    target_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'systemds', 'operator', 'algorithm', 'builtin')
    source_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))),'scripts', 'builtin')
    template_path = os.path.join('resources', 'template_python_script_imports')
    init_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'systemds', 'operator', 'algorithm', '__init__.py')
    init_import = u"from .builtin.{function} import {function} \n"
    init_all = u"__all__ = {functions} \n"

    def __init__(self, extension: str='py'):
        super(PythonAPIFileGenerator, self).__init__()
        self.extension = '.{extension}'.format(extension=extension)
        os.makedirs(self.__class__.target_path, exist_ok = True)
        self.function_names = list()

    def generate_file(self, filename: str, file_content: str):
        """
        Generates file in self.path with name file_name
        and given file_contents as content
        """
        path = os.path.dirname(__file__)
        template_license_import_path = os.path.join(path, self.__class__.template_path)
        with open(template_license_import_path, 'r') as temp_py_script:
            license_imports = temp_py_script.read()

        target_file = os.path.join(self.target_path, filename) + self.extension
        with open(target_file, "w") as new_script:
            new_script.write(license_imports)
            new_script.writelines("\n \n")
            new_script.write(file_content)

        self.function_names.append(filename)

    def generate_init_file(self):
        with open(self.init_path, "w") as init_file:
            for f in self.function_names:
                init_file.write(self.init_import.format(function=f))

            init_file.write("\n")
            init_file.write(self.init_all.format(functions=self.function_names).replace("'", ""))


class PythonAPIFunctionGenerator(object):

    api_template = u"""def {function_name}({parameters}) -> OperationNode:
    {header}
    {value_checks}
    {params_dict}
    return {api_call}\n\n
    """

    kwargs_parameter_string = u"**kwargs: Dict[str, VALID_INPUT_TYPES]"
    kwargs_result = u"params_dict.update(kwargs)"

    value_check_template = u"\n    {param}._check_matrix_op()"
    type_mapping_file = os.path.join('resources','type_mapping.json')

    type_mapping_pattern = r"^([^\[\s]+)"

    path = os.path.dirname(__file__)
    type_mapping_path = os.path.join(path, type_mapping_file)
    # print(type_mapping_path)
    with open(type_mapping_path, 'r') as mapping:
        type_mapping = json.load(mapping)

    def __init__(self):
        super(PythonAPIFunctionGenerator, self).__init__()

    def generate_function(self, data:dict) -> str:
        """
        Generates function definition for PythonAPI
        @param data:
            {
                'function_name': 'some_name',
                'function_header': 'header contained in \"\"\"'
                'parameters': [('param1','type','default_value'), ...],
                'return_values': [('retval1', 'type'),...]
            }
        @return: function definition
        """
        parameters = self.format_param_string(data['parameters'])
        function_name = data['function_name']
        header = data['function_header'] if data['function_header'] else ""
        value_checks = self.format_value_checks(data['parameters'])
        params_dict = self.format_params_dict_string(data['parameters'])
        api_call = self.format_api_call(
            data['parameters'],
            data['return_values'],
            data['function_name']
        )
        #print(parameters)
        #print(function_name)
        #print(header)
        #print(value_checks)
        #print(params_dict)
        #print(api_call)
        return self.__class__.api_template.format(
            function_name=function_name,
            parameters=parameters,
            header=header,
            value_checks=value_checks,
            params_dict=params_dict,
            api_call=api_call
        )

    def format_param_string(self, parameters: List[Tuple[str]]) -> str:
        result = u""
        has_optional = False
        path = os.path.dirname(__file__)
        for param in parameters:
            # map data types
            pattern = self.__class__.type_mapping_pattern
            param = tuple([self.__class__.type_mapping["type"].get(re.search(pattern, str(item).lower()).group() if item else str(item).lower(), item) for item in param])
            if param[2] is not None:
                has_optional = True
            else:
                if len(result):
                    result = u"{result}, ".format(result=result)
                result = u"{result}{name}: {typ}".format(
                    result=result,
                    name=param[0],
                    typ=param[1]
                )
        if has_optional:
            if len(result):
                result = u"{result}, ".format(result=result)
            result = u"{result}{kwargs}".format(
                result=result,
                kwargs=self.__class__.kwargs_parameter_string
            )
        return result

    def format_params_dict_string(self, parameters: List[Tuple[str]]) -> str:
        if not len(parameters):
            return ""
        has_optional = False
        result = ""
        for param in parameters:
            if param[2] is not None:
                has_optional = True
            else:
                if len(result):
                    result = u"{result}, ".format(
                        result=result)
                else:
                    result = u"params_dict = {"
                result = u"{result}\'{name}\':{name}".format(
                    result=result,
                    name=param[0]
                )
        result = u"{result}}}".format(result=result)
        if has_optional:
            result = u"{result}\n    {kwargs}".format(
                result=result,
                kwargs=self.__class__.kwargs_result
            )
        return result

    # TODO: shape parameter, mapping of return type
    def format_api_call(
        self,
        parameters: List[Tuple[str]],
        return_values: List[Tuple[str]],
        function_name: str
        ) -> str:
        length = len(return_values)
        result = "OperationNode({params})"
        param_string = ""
        param = parameters[0]
        if length > 1:
            output_type_list = ""
            for value in return_values:
                output_type = ".".join(re.split("[\[\]]", value[1])).upper()[:-1]
                print(output_type)
                if len(output_type_list):
                    output_type_list = "{output_type_list}, ".format(
                        output_type_list=output_type_list
                    )
                else:
                    output_type_list = "output_types=["
                
                output_type_list = "{output_type_list}OutputType.{typ}".format(
                    output_type_list=output_type_list,
                    typ=output_type
                )
            output_type_list = "{output_type_list}]".format(
                output_type_list=output_type_list
            )
            output_type = "LIST, number_of_outputs={n}, {output_type_list}".format(
                n=length,
                output_type_list=output_type_list
            )
        else:
            value = return_values[0]
            output_type = ".".join(re.split("[\[\]]", value[1])).upper()[:-1]
            # print(output_type)
        result = "{param}.sds_context, \'{function_name}\', named_input_nodes=params_dict, " \
                 "output_type=OutputType.{output_type}".format(
            param=param[0],
            function_name=function_name,
            output_type=output_type
        )
        result = "OperationNode({params})".format(params=result)
        return result

    def format_value_checks(self, parameters :List[Tuple[str]]) -> str:
        result = ""
        for param in parameters:
            if "matrix" not in param[1].lower():
                # This check is only needed for Matrix types
                continue
            check = self.__class__.value_check_template.format(param=param[0])
            result = "{result}{check}".format(
                result=result,
                check=check
            )
        return result
    

class PythonAPIDocumentationGenerator(object):
    python_multi_cmnt = "\"\"\""
    param_str = "\n    :param {pname}: {meaning}"
    return_str = "\n    :return: \'OperationNode\' containing {meaning} \n"

    def __init__(self):
        super(PythonAPIDocumentationGenerator, self).__init__()

    def generate_documentation(self, data:dict) -> str:
        """
        Generates function header for PythonAPI
        @param data:
            {
                'function_name': 'some_name',
                'parameters': [('param1','description'), ...],
                'return_values': [('retval1', 'descritpion'),...]
            }
        @return: function header including '\"\"\"' at start and end
        """
        input_param = self.header_parameter_string(data["parameters"])
        output_param = self.header_return_string(data["return_values"])

        return self.__class__.python_multi_cmnt + input_param + output_param + "    " + self.__class__.python_multi_cmnt

    def header_parameter_string(self, parameter:dict) -> str:
        parameter_str = ""
        for param in parameter:
            parameter_str += self.__class__.param_str.format(pname=param[0], meaning=param[3])

        return parameter_str

    def header_return_string(self, parameter:dict) -> str:
        meaning_str = ""

        for param in parameter:
            if len(meaning_str) > 0:
                meaning_str += " & " + param[3]
            else:
                meaning_str += param[3]

        return self.__class__.return_str.format(meaning=meaning_str.lower())


if __name__ == "__main__":
    f_parser = FunctionParser(PythonAPIFileGenerator.source_path)
    doc_generator = PythonAPIDocumentationGenerator()
    fun_generator = PythonAPIFunctionGenerator()
    file_generator = PythonAPIFileGenerator()

    for dml_file in f_parser.files():
        try:
            header_data = f_parser.parse_header(dml_file)
            data = f_parser.parse_function(dml_file)
            # TODO: define a set of dml script that would not fail this check
            f_parser.check_parameters(header_data, data)
        except Exception as e:
            # print("[WARNING] Skipping file \'{file_name}\'.".format(file_name = dml_file))
            continue
        data['function_header'] = doc_generator.generate_documentation(header_data)
        script_content = fun_generator.generate_function(data)
        file_generator.generate_file(data["function_name"], script_content)

    file_generator.generate_init_file()



