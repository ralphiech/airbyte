import json
import os
import re


path = "C:\\Users\\ZoranStipanicev\\Documents\\vtcm_ddl_scripts"
output_file_name = "sample_catalog.json"
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
for file in os.listdir(path):
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
        f = open(os.path.join(path, filename))
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
with open(f"{path}\\{output_file_name}", "w") as write_file:
    write_file.write(output)
    write_file.close()
