# IFS
image storage system，基于[tikv](https://github.com/pingcap/tikv),同时参考[bfs](https://github.com/Terry-Mao/bfs)实现的分布式小文件(图片)存储系统 <br>
## 运行步骤 <br>
### 1、编译、启动pd <br>
git clone https://github.com/scottzzq/pd  <br>
cd pd <br>
make <br>
./bin/pd-server <br>

### 2、编译store server <br>
git clone https://github.com/scottzzq/IFS <br>
make <br>
sh start_cluster.sh <br>

### 3、用python client发送上传、下载、删除图片请求 <br>
git clone https://github.com/scottzzq/kvproto <br> 
cd kvproto/py_src/ <br>
python cmd.py, 可以自己修改cmd.py <br>

## 优化
- [x] 优化raftlog存储格式，考虑到图片数据文件较大，故总是将raftlog中的PUT请求中的数据转换Needle写入volume文件中，raftlog只保存Needle在volume中的offset&size
- [x] 上传图片
- [x] 下载图片
- [x] 删除图片
- [x] 动态增加Volume
- [x] 动态增加store机器
- [ ] Compact Volume，将已经删除的图片Needle文件回收空间
- [ ] go实现http proxy，提供http上传、下载、删除接口
- [ ] 图片元信息存储，filename和volumeid&key映射关系，考虑使用类redis的硬盘存储介质，360开源[pika](https://github.com/Qihoo360/pika)可能是一个不错的选择
