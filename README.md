# IFS
image storage system，基于[tikv](https://github.com/pingcap/tikv)实现的分布式小文件(图片)存储系统
##运行步骤
###1、编译、启动pd
git clone https://github.com/scottzzq/pd 
cd pd
make
./bin/pd-server

###2、编译store server
git clone https://github.com/scottzzq/IFS
make
sh start_cluster.sh

###3、用python client发送上传、下载、删除图片请求
git clone https://github.com/scottzzq/kvproto <br> 
cd kvproto/py_src/
python cmd.py, 可以自己修改cmd.py
