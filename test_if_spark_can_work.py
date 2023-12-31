from pyspark import SparkContext
logFile = "input"  
sc = SparkContext("local", "first app")
logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print(f"Lines with a:{numAs}, lines with b:{numBs}")

"""
hadoop@ubuntu:~/bigdata$ cd ~
hadoop@ubuntu:~$ cd /usr/local/hadoop/
hadoop@ubuntu:/usr/local/hadoop$ ./sbin/start-dfs.sh
"""