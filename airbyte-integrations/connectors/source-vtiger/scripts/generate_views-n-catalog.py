import json
import os
import re
from pathlib import Path


def generate_views():
    cwd = os.getcwd()
    path_schemas = os.path.abspath(os.path.join(cwd, f'source_vtiger{os.sep}schemas'))
    path_ddl_out = os.path.abspath(os.path.join(cwd, 'output-ddl'))

    # create the output directory if it does not exist yet
    Path(path_ddl_out).mkdir(exist_ok=True)

    ev_func_body = ""
    print("path_schemas")
    print(path_schemas)
    if os.path.exists(path_schemas) == False:
        print("path not found")
        print(path_schemas)
    else:
        print("path found!!")
        print(path_schemas)

    p_end = re.compile('_+$')
    p_mid = re.compile('_{2,}')

    for file in os.listdir(path_schemas):
        # print(file)
        filename = os.fsdecode(file)
        if filename == "me.json" or filename.endswith("catalog.json"):
            continue # we are skipping me.json as it's not needed for reports

        if filename.endswith(".json"):
            print(filename)
            f = open(os.path.join(path_schemas, filename))
            data = json.load(f)
            # print(data)
            fields_dict = data["properties"]["result"]["items"][0]["properties"]
            # print(fields_dict)
            view_fields = []
            ########################################################################################
            # field_alias is used to avoid duplicate aliases / view column names
            # we are taking labels from vtiger for alias and 2 fields can have same label in vTiger
            # that's OK in vTiger but we can't have to columns with the same name
            field_alias = {}

            for item in fields_dict:
                alias = ""
                # not all fileds have title and description defined in json schema
                db_name = item
                field_name = item
                if len(field_name) > 43:
                    # airbyte logic to deal with long names
                    field_name = field_name[0:20] + '__' + field_name[-21:]
                # if field name in the table differs from the schema name
                # if "db_name" in fields_dict[item]:
                #     db_name = fields_dict[item]["db_name"]

                if "title" in fields_dict[item]:
                    title = fields_dict[item]["title"]
                else:
                    title = item

                type = 'varchar'
                if "db_type" in fields_dict[item]:
                    type = fields_dict[item]["db_type"]

                # we need to clean the title and convert to lower to be sure we will avoid duplicate column names
                title_lower = title.lower()
                replace_chars = " ()';,&.?!-:"
                mapping = alias.maketrans(replace_chars, "_" * len(replace_chars))
                title_lower = title_lower.translate(mapping)
                title_lower = p_end.sub("", title_lower)

                # print(title_lower)
                if title_lower in field_alias:
                    alias = item # title was already used as an field alias so we keep vTiger field name as view column name (alias)
                else:
                    field_alias[title_lower] = item
                    alias = title

                if "description" in fields_dict[item]:
                    desc = fields_dict[item]["description"]
                else:
                    desc = item

                alias = p_mid.sub("_", alias.translate(mapping))
                alias = p_end.sub("", alias)
                alias = alias[0:59] # pg limit on column name length
                # if filed name contains & we need to wrap it into double quotes
                if "&" in field_name:
                    field_name = f'"{field_name}"'
                view_fields.append({"field": field_name, "desc": desc, "alias": alias, "type": type})
                # print(view_fields[item])
                # print("built in entity")
            # print(field_alias)

            # print(view_fields)
            common_path_prefix = f"{path_ddl_out}{os.sep}"

            short_view_name = filename.replace("vtcm_", "").replace(".json", "")
            view_name = "reports." + short_view_name
            create_view = "CREATE OR REPLACE VIEW " + view_name + " AS" + "\nSELECT "
            i = 0
            comments = ""
            for v in view_fields: #.items():
                field = v['field']
                alias = v['alias']
                desc = v["desc"].replace("'", "''")

                print(f"field: {field} > type: {v['type']}")
                if v['type'] != 'varchar':
                    field = "NULLIF(" + field + ", '')::" + v['type']


                if i > 0:
                    create_view = create_view + "\n     , " +field + " AS " + alias
                else:
                    create_view = create_view + field + " AS " + alias
                i = i+1

                comments = comments + f'\ncomment on column {view_name}.{alias} is \'{desc}\';'
            source_table_name = "public." + filename.replace(".json", "") + "_result"
            create_view = create_view + f"\n  FROM {source_table_name};\n"
            create_view = create_view + comments
            print(create_view)

            # if you need a file with only the view creation uncomment this [for debugging]
            # with open(common_path_prefix + "dbg_vw_" + short_view_name + ".sql", "w") as write_file:
            #     write_file.write(create_view)

            procedure_name = f"public.prc_create_vw_{short_view_name}"
            create_procedure = f"CREATE OR REPLACE PROCEDURE {procedure_name}() AS\n$$\n" + create_view
            create_procedure = create_procedure + "\n$$\nLANGUAGE sql;"

            with open(f"{common_path_prefix}01_prc_create_vw_{short_view_name}.sql", "w") as write_file:
                write_file.write(create_procedure)

            body_append = f"""      IF r.object_identity = '{source_table_name}'
          THEN
            CALL {procedure_name}();
          END IF;
    """
            ev_func_body = ev_func_body + body_append
        else:
            continue

    event_function = f"""CREATE OR REPLACE FUNCTION fun_ev_create_views()
     RETURNS event_trigger
     LANGUAGE plpgsql VOLATILE AS
    $$
    DECLARE
      r RECORD;
    BEGIN
      FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
    {ev_func_body}
      END LOOP;
    END;
    $$;
    """

    with open(f"{common_path_prefix}02_event_function.sql", "w") as write_file:
        write_file.write(event_function)

    event_trigger_ddl = """DROP EVENT TRIGGER IF EXISTS ev_create_views;
    CREATE EVENT TRIGGER ev_create_views ON ddl_command_end WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE', 'CREATE TABLE AS')
    EXECUTE FUNCTION  public.fun_ev_create_views();
    """

    with open(f"{common_path_prefix}03_ev_trg_create_views.sql", "w") as write_file:
        write_file.write(event_trigger_ddl)

    schema_ddl = """CREATE SCHEMA IF NOT EXISTS reports AUTHORIZATION postgres;"""

    with open(f"{common_path_prefix}00_create_schema.sql", "w") as write_file:
        write_file.write(schema_ddl)

def generate_catalog():
    cwd = os.getcwd()
    path_schemas = os.path.abspath(os.path.join(cwd, f'source_vtiger{os.sep}schemas'))
    path_catalog_out = os.path.abspath(os.path.join(cwd, f'sample_files'))
    output_file_name = "configured_catalog.json"
    catalog_hdr = """{
      "streams": ["""
    catalog_ftr = """
      ]
    }
    """
    catalog_element_pre = """   <,>{
          "sync_mode": "full_refresh",
          "destination_sync_mode": "overwrite",
          "stream":
          {
            "name": "<entity-name>",
            "supported_sync_modes": [
              "full_refresh"
            ],
            "source_defined_cursor": false,
            "json_schema":
    """
    catalog_element_pos = """      }
        }"""
    file_count = 0
    catalog_bdy = ""
    for file in os.listdir(path_schemas):
        file_count = file_count + 1
        # print(file)
        filename = os.fsdecode(file)
        # if filename == "me.json":
        #     continue # we are skipping me.json as it's not needed for reports
        element = ""
        if filename.endswith(".json"):
            if filename == output_file_name:
                continue
            entity_name = filename.replace(".json", "")
            print(filename)
            f = open(os.path.join(path_schemas, filename))
            data = f.read()
            data = (' '*10).join((' '*10+data).splitlines(True))
            # TODO
            # ADD spaces to beginning of every line

            # print(data)
            if file_count == 1:
                element = catalog_element_pre.replace("<,>", " ")
            else:
                element = "\n" + catalog_element_pre.replace("<,>", ",")
            element = element.replace("<entity-name>", entity_name)
            element = element + data + catalog_element_pos
            catalog_bdy = catalog_bdy + element

    print("::: Loop End :::")
    output = catalog_hdr + catalog_bdy + catalog_ftr
    print(len(output))
    with open(f"{path_catalog_out}{os.sep}{output_file_name}", "w") as write_file:
        write_file.write(output)
        write_file.close()

generate_views()
generate_catalog()
# print(os.getcwd())

# cwd = os.getcwd()
# path_schemas = os.path.join(cwd, f'source-vtiger{os.sep}schemas')
# print(path_schemas)
# print(os.path.abspath(path_schemas))

# # path_schemas = f"source-vtiger/schemas"
# path_ddl_out = os.path.join(cwd, 'output-ddl')
# print(path_ddl_out)
