# MGR 一键部署工具
## 环境依赖与使用说明

### 1. 安装 Python 依赖
```bash
pip3 install paramiko openpyxl pandas pyinstaller
```

### 2. 项目打包
```bash
pyinstaller main.spec
```

### 3. Linux 服务器运行
上传打包后的二进制文件至服务器，执行以下命令：
```bash
[root@localhost ~]# ./MGR.BIN
```
