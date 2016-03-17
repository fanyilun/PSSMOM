#!/bin/bash  
#引入java环境变量  
. /etc/profile  
  
#取得当前.sh文件所在的目录  
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"  

#引入class所在的文件夹  
classes=$DIR/target/classes  
#lib folder    
libs=$DIR/lib

#将classes和包jar加入classpath，注意用的是冒号":"分割的  
classpath=$classes:$libs/asm-5.0.3.jar:$libs/commons-pool2-2.4.2.jar:$libs/hessian-4.0.7.jar:$libs/junit-4.12.jar:$libs/kryo-2.24.0.jar:$libs/kryo-3.0.3.jar:$libs/kryo-serializers-0.35.jar:$libs/minlog-1.2.jar:$libs/minlog-1.3.0.jar:$libs/netty-all-4.0.23.Final.jar:$libs/objenesis-2.1.jar:$libs/protobuf-java-2.6.1.jar:$libs/reflectasm-1.10.1.jar:$libs/mom-api-1.0.jar

# 执行java的调用过程，格式如下：  
# java -classpath $classpath 主函数类入口 
java -classpath $classpath fyl.middleware.mom.broker.Broker #>> ~/output.log
echo "shell over.." 