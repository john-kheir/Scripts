# This script take the folder for loggify and data dict and compare if all fields in the data dict
# And if its data types as expected

import os
import sys
import json
import re

upper_count = 0
fields_not_in_data_dict = []
filters = []

def get_field_and_its_type(field, data_dict, line, file, line_number):
    # will return the data_type if the field exists and will return no_type otherwise
    global upper_count
    global filters

    def _get_type(type_part):
        global upper_count

        if type_part.startswith("str(") or type_part.startswith("lowercase(") or type_part.startswith("\""):
            f_type = "string"
            if field.endswith("id") and (not type_part.startswith("str(")) and field != "event_id":
                upper_count = upper_count + 1
                print(f"{upper_count}: field: {field} should be string in filter {file} in line: {line_number}")
                filters.append(file)
            if (not field.endswith("id")) and type_part.startswith("str("):
                upper_count = upper_count + 1
                print(f"{upper_count}- field: {field} should be lowercased in filter {file} in line: {line_number}")
                filters.append(file)
        elif type_part.startswith("@data_source"):
            f_type = "string"
        elif type_part.startswith("int("):
            f_type = "int"
        elif type_part.startswith("true") or type_part.startswith("false"):
            f_type = "bool"
        elif type_part.startswith("flt("):
            f_type = "float"
        elif type_part.startswith("datetime("):
            f_type = "timestamp"
        elif type_part.startswith("ipv4(") or type_part.startswith("ipv6("):
            f_type = "ipaddress"
        #elif type_part.startswith("remove_all("):
        #    #import ipdb;
        #    #ipdb.set_trace()
        #    p_type = _get_type(type_part.split("(")[1].strip())
        #    f_type = p_type

        elif type_part.startswith("map("):
            array_type = _get_type(type_part.split(",")[1].strip())
            f_type = f"array<{array_type}>"
        elif type_part.startswith("["):
            array_type = _get_type(type_part.split("[")[1].strip())
            f_type = f"array<{array_type}>"
        elif type_part.startswith("flatten("):
            f_type = "flatten(array)"
        else:
            return "no_type"
        return f_type

    if field in data_dict.keys():
        type_part = line.split(" from ")[1]
        type_part = type_part.strip()
        loggify_field_type = _get_type(type_part)
        data_dict_field_type = data_dict[field]["data_type"]

        if not (loggify_field_type == data_dict_field_type):
            if not (loggify_field_type == f"array<{data_dict_field_type}>"):
                upper_count = upper_count + 1
                print(f"{upper_count}- Loggify field '{field}' of type: '{loggify_field_type}' does not equal data_dict field type:"
                      f" '{data_dict_field_type}' for filter: '{file}' in line {line_number}")
                if file == "trendmicro_cloud_app_securit.loggify":
                    import ipdb;ipdb.set_trace()
                filters.append(file)
    else:
        # this means field is not found in the data dict
        # return False
        upper_count = upper_count + 1
        message = f"Field '{field}' in filter '{file}' in line: {line_number} does not exist in the data dict"

        global fields_not_in_data_dict
        fields_not_in_data_dict.append(message)
        print(message)
        filters.append(file)


def load_data_dict(data_dict_file):
    f = open(data_dict_file, )
    data_dict = json.load(f)
    return data_dict


def check_fields_and_its_data_type(loggify_files_path, data_dict_path):
    data_dict = load_data_dict(data_dict_path)
    # reading loggify files
    files = os.listdir(loggify_files_path)

    for file in files:
        if file.endswith("loggify"):
            file1 = open(loggify_files_path + file, 'r')
            lines = file1.readlines()

            line_number = 0
            for line in lines:
                tabs_output = re.search("\t+output ", line) # to match output in case it has tabs
                l_stripped_line = line.lstrip()
                line_number += 1
                if not (l_stripped_line.startswith("#") or l_stripped_line.startswith("//")): # if not comment
                    if (tabs_output or " output " in line) and " from " in line:
                        if tabs_output:
                            f = line.split(tabs_output.group(0))[1].split(" from ")[0]
                        else:
                            f = line.split(" output ")[1].split(" from ")[0]
                        field = f.strip().strip("|")
                        if "concatenated event_context" in field:
                            continue
                        get_field_and_its_type(field, data_dict, line, file, line_number)
    global filters
    error_filters = list(set(filters))
    print(f"There are {len(error_filters)} filters that has errors: {error_filters}")



if __name__ == "__main__":
    upper_count = 0
    # loggify_folder_path
    loggify_files_path = sys.argv[1]
    # if loggify_files_path  doesn't end with slash, add slash
    if not loggify_files_path.endswith("/"):
        loggify_files_path = loggify_files_path + "/"

    # data dict: single source of truth
    data_dict_path = sys.argv[2]


    check_fields_and_its_data_type(loggify_files_path, data_dict_path)