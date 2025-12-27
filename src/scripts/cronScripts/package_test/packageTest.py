import sys

# import pymysql
sys.path.insert(1, '') # para poder modularizar el código, separándolo en diferentes carpetas
sys.path.append("")
sys.path.append("../../..")
sys.path.append("")
sys.path.append("/home/")



def printHola(numero):
    return "holas " +str(1+numero)
