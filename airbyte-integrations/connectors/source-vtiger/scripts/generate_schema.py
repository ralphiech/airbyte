import json
import os
import argparse
import json
import sys



# add the vtiger modules you want to create schemas for here.
# valid inputs are what the following endpoint returns: 
# restapi/v1/vtiger/default/listtypes?fieldTypeList=nullParameters
vtiger_types = [
    # "Leads",
    # "Contacts",
]



parser = argparse.ArgumentParser()
parser.add_argument('config_file', help='please provide a config.json file')
parser.add_argument('destination', help='please provide a destination directory where the output files should be stored')

args = parser.parse_args()
if not os.path.isfile(args.config_file):
    sys.exit("the provided config.json file does not exist")

with open(args.config_file) as json_file:
    config = json.load(json_file)

if os.path.isdir(args.destination):
    for type in vtiger_types:
        destination_path = "{}/{}.json".format(args.destination, type.lower())
        print("generating: {}".format(destination_path))
        cmd = "curl -s -u {}:{} https://{}/restapi/v1/vtiger/default/query?query=select%20*%20from%20{}%3B | genson | jq . > {}"
        os.system(cmd.format(config['username'], config['accessKey'], config['host'], type, destination_path))
else:
    print("you did not provide a valid directory")
