### 启动集群（3个DragonBoat Raft节点）
```
./basalt -port 18419 -peers localhost:63001,localhost:63002,localhost:63003 -nodeid 1
./basalt -port 28419 -peers localhost:63001,localhost:63002,localhost:63003 -nodeid 2
./basalt -port 38419 -peers localhost:63001,localhost:63002,localhost:63003 -nodeid 3
```

### 测试
添加数据（采用http访问方式）
```
curl -XPOST http://127.0.0.1:18419/add/test/1000
```

检查数据（采用http访问方式）
```
curl http://127.0.0.1:38419/exists/test/1000
```