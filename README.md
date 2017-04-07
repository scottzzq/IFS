# IFS
image storage system，基于[tikv](https://github.com/pingcap/tikv)实现的分布式小文件(图片)存储系统 <br>
##运行步骤 <br>
###1、编译、启动pd <br>
git clone https://github.com/scottzzq/pd  <br>
cd pd <br>
make <br>
./bin/pd-server <br>

###2、编译store server <br>
git clone https://github.com/scottzzq/IFS <br>
make <br>
sh start_cluster.sh <br>

###3、用python client发送上传、下载、删除图片请求 <br>
git clone https://github.com/scottzzq/kvproto <br> 
cd kvproto/py_src/ <br>
python cmd.py, 可以自己修改cmd.py <br>
