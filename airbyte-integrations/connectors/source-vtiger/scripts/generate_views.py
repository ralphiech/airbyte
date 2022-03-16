import json
import os
import re

path = "C:\\Users\\ZoranStipanicev\\Documents\\vtcm_ddl_scripts"
ev_func_body = ""
for file in os.listdir(path):
    # print(file)
    filename = os.fsdecode(file)
    if filename == "me.json" or filename.endswith("catalog.json"):
        continue # we are skipping me.json as it's not needed for reports

    if filename.endswith(".json"):
        print(filename)
        f = open(os.path.join(path, filename))
        data = json.load(f)
        # print(data)
        fields_dict = data["properties"]["result"]["items"][0]["properties"]
        print(fields_dict)
        view_fields = {}
        field_alias = {}
        for item in fields_dict:
            # print(fields_dict[item])
            # if filename.startswith("vtcm"):
            #     print("custom entity")
            # else:
            alias = ""
            # not all fileds have title and description defined in json schema
            db_name = item
            # if field name in the table differs from the schema name
            if "db_name" in fields_dict[item]:
                db_name = fields_dict[item]["db_name"]

            if "title" in fields_dict[item]:
                title = fields_dict[item]["title"]
            else:
                title = item
            if title in field_alias:
                alias = item
            else:
                field_alias[title] = item
                alias = title
            if "description" in fields_dict[item]:
                desc = fields_dict[item]["description"]
            else:
                desc = item

            mapping = alias.maketrans(" ().?!-","_______")
            view_fields[db_name] = {"field": item, "desc": desc, "alias": re.sub("_{2,}", "_", alias.translate(mapping) )}
            # print(view_fields[item])
            # print("built in entity")
        print(view_fields)
        short_view_name = filename.replace("vtcm_", "").replace(".json", "")
        view_name = "reports." + short_view_name
        create_view = "CREATE OR REPLACE VIEW " + view_name + " AS" + "\nSELECT "
        i = 0
        comments = ""
        for k, v in view_fields.items():
            if i > 0:
                create_view = create_view + "\n     , " + k + " AS " + v["alias"]
            else:
                create_view = create_view + k + " AS " + v["alias"]
            i = i+1
            comments = comments + f'\ncomment on column {view_name}.{v["alias"]} is \'{v["desc"]}\';'
        source_table_name = "public." + filename.replace(".json", "") + "_result"
        create_view = create_view + f"\n  FROM {source_table_name};\n"
        create_view = create_view + comments
        print(create_view)
        with open(path + "\\vw_" + short_view_name + ".sql", "w") as write_file:
            write_file.write(create_view)

        procedure_name = f"public.prc_create_vw_{short_view_name}"
        create_procedure = f"CREATE OR REPLACE PROCEDURE {procedure_name}() AS\n$$\n" + create_view
        create_procedure = create_procedure + "\n$$\nLANGUAGE sql;"

        with open(f"{path}\\prc_create_vw_{short_view_name}.sql", "w") as write_file:
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

with open(f"{path}\\event_function.sql", "w") as write_file:
    write_file.write(event_function)
