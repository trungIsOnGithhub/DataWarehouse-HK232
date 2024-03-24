cd /opt/integrate-sales-data
export LUIGI_CONFIG_PATH=luigi_dev.cfg
python -m luigi --module main
IntegrateSalesData >> luigi_dev.log 2>&1