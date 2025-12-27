import datetime
import sys, os
now =datetime.datetime.now()
print("empezando configuración del entorno para ejecutar los demonios ", now)

os.system('cron -l 2 -f')
print('cron configurado___')

os.system('printenv > /etc/environment')
print('variable entorno configurada___')

print("Acabamos de configurar el entorno ")

