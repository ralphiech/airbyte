# echo 'delete old schema files'
# echo '***********************'
# rm source_vtiger/schemas/*.json -v

# echo 'generate new schema files'
# echo '*************************'
# python3 ./scripts/generate_schemas.py





echo 'remove old ddl (sql) files'
echo '***********************'
rm output-ddl/*.sql -v

echo 'remove old configured_catalog.json'
echo '***********************'
rm sample_files/configured_catalog.json

echo 'generate configured_catalog.json and ddl files'
echo '*************************'
python3 ./scripts/generate_views-n-catalog.py