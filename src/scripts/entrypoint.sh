#!/bin/sh
#echo "configurando repositorio dvc"
#dvc init -q --no-scm
#dvc remote add -d minio_remlops s3://dvcbucket
#dvc remote modify minio_remlops endpointurl http://172.24.0.5:9000
##dvc remote show minio_remlops
#echo "modificada variable endpoint"

echo "se va a crear la estructura de datos faltante"
mkdir -p data/experimentResults
mkdir -p data/experimentDatasets
mkdir -p data/dataBaseTest
mkdir -p data/testingData
mkdir -p logs/cronLogs

echo "Crontab contents:"
cat /var/spool/cron/crontabs/cronsGesth

# Create a custom log file for cron
touch /var/log/cron_custom.log
chmod 777 /var/log/cron_custom.log
# -------------------------------------
touch /var/log/cron.log
chmod 777 /var/log/cron.log
chown cronsGesth /var/log/cron.log
chmod -R 777 /home/ETL-PCP/logs/cronLogs/
chown -R cronsGesth /home/ETL-PCP/logs/cronLogs/
# -------------------------------------


crontab -u cronsGesth /var/spool/cron/crontabs/cronsGesth

# Start cron in the background with custom log file
cron -L 15 -f > /var/log/cron_custom.log 2>&1 &

# service cron stop
# service cron start



# Check if cron is running
echo "Cron status:"
pgrep cron

if tail -n 1 /var/spool/cron/crontabs/cronsGesth | read -r _; then
  echo "There is a newline at the end of the root2 file."
else
  echo "There is no newline at the end of the root2 file."
fi


#echo "Vamos a configurar git"
#chown -R $(whoami) .
#git config --global user.email "a52hepof@uco.es"
#git config --global user.name "a52hepof"
#echo "https://user:token@dagshub.com" > .git-credentials
#git config credential.helper store
#git config --global credential.helper cache
#git status


# Keep the container  running
tail -f /dev/null
