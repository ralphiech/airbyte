import json
import os
import re
import requests
from requests.auth import HTTPDigestAuth

def replce_if_dict_elem_exists(in_string, in_dict, in_label ):
    ret = in_string
    if in_label in in_dict:
        ret = in_string.replace(f"<{in_label}>", in_dict[in_label])
    return ret

##########################################################################

def gen_schema(in_json_details):
    schema_config = read_json_secrets_file("schema.json")

    fields_config = {}
    if 'fields_config' in schema_config:
        print('fields_config defined')
        fields_config = schema_config['fields_config']

    schema_prefix = """{
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "success": {
          "type": "boolean"
        },
        "result": {
          "type": "array",
          "items": [
            {
              "type": "object",
              "properties": {
    """
    schema_postfix = """          }
            }
          ]
        }
      }
    }
    """
    field_template = """            "<name>": {
                  "type": "string",
                  "title": "<label>",
                  "description": "<dblabel>",
                  "default": ""<db_name>
                }
    """

    data = in_json_details
    print(data)
    entity_name = data["result"]["name"]
    print(entity_name)
    print("========================================")
    all_fields = ""
    for fld in data["result"]["fields"]:
        db_name = ""
        fld_name = fld["name"]
        if entity_name in fields_config:
            if fld_name in fields_config[entity_name]:
                if "db_name" in fields_config[entity_name][fld_name]:
                    db_name = f',\n                  "db_name": "{fields_config[entity_name][fld_name]["db_name"]}"'

        # print(fld_name)
        field_json = field_template
        field_json = replce_if_dict_elem_exists(field_json, fld, "name")
        field_json = replce_if_dict_elem_exists(field_json, fld, "label")
        field_json = replce_if_dict_elem_exists(field_json, fld, "dblabel")
        field_json = field_json.replace('<db_name>', db_name)
        all_fields = all_fields + field_json

    all_fields = schema_prefix + all_fields + schema_postfix
    # print(schema_postfix)
    # print(all_fields)
    return all_fields

##########################################################################

def get_entity_details(in_entity):
    print("=== get_entity_details ===")
    conn_config = read_json_secrets_file("config.json")
    entity_name = in_entity

    username = conn_config["username"]
    password = conn_config["accessKey"]
    host = conn_config["host"]

    url = f"https://{host}/restapi/v1/vtiger/default/describe?elementType={entity_name}"


    response = requests.get(url, auth=(username, password))

    # print("response.status_code")
    # print(response.status_code)
    # print("response.text")
    # print(response.text)
    return json.loads(response.text)

##########################################################################

def write_schema(in_content, in_entity):
    print("write_schema :: " + in_entity)
    out_file_name = in_entity
    cwd = os.getcwd()
    path_schemas = os.path.abspath(os.path.join(cwd, f'..{os.sep}source_vtiger{os.sep}schemas'))

    file_path = os.path.join(path_schemas, out_file_name)
    with open(file_path, "w") as write_file:
        write_file.write(in_content)

##########################################################################

def parse_source_script():
    in_source = f'..{os.sep}source_vtiger{os.sep}source.py'

    schema_file_names = []
    query_names = []
    p = re.compile('([A-Z])')
    with open(in_source, 'r') as f:
        for line in f:
            found = re.findall("^class (.+)\(VtigerStream\)", line)
            if found:
                schema_file_name = found[0][0] + p.sub(r'_\1', found[0][1:])
                schema_file_name = schema_file_name.lower()
                # print(schema_file_name)
                schema_file_names.append(schema_file_name)
            found = re.findall("return self\.get_query_url_string\(\'(.+)\', next_page_token\)", line)
            if found:
                query_names.append(found[0])

    combined = []
    for (s, q) in zip(schema_file_names, query_names):
        combined.append({"query_name":q, "schema_file_name":s})
    return combined

##########################################################################

def main():
    # add the vtiger modules you want to create schemas for here.
    # valid inputs are what the following endpoint returns:
    # restapi/v1/vtiger/default/listtypes?fieldTypeList=nullParameters

    # sample schema config file
    # {
    #   "fields_config": {
    #     "Calendar": {
    #       "a_field_with_a_really_long_name_and_airbyte_shortens_it_in_db": {
    #         "db_name": "a_field_with_a_rea_airbyte_shortens_it_in_db"
    #       }
    #     }
    #   }
    # }

    print("=== Main ===")
    # schema_config = read_json_secrets_file("schema.json")
    entities = parse_source_script()
    print(len(entities))
    if len(entities) == 0:
        print("No classes recognised in the source.py file")
    else:
        # vtiger_types = schema_config["vtiger_types_list"]
        for entity in entities:
            entity_name = entity["query_name"]
            print(f"processing: {entity_name}")
            entity_details = get_entity_details(entity_name)
            schema = gen_schema(entity_details)
            write_schema(schema, entity["schema_file_name"])

main()

####
# TODO
# Add support in the config file for fields which are not in
# the describe end point output
