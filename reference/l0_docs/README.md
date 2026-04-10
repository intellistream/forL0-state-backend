# 25.09版本(调试版）
## 修改bios
在BIOS中设置cache mode为in shared out shared模式

## 安装内核

下载内核安装包：kernel-5.10.0_l0_mwp+-80.aarch64.rpm


```
rpm -ivh kernel-5.10.0_l0_mwp+-80.aarch64.rpm
```

## 安装驱动

下载驱动：hisi_l0_mem_pool.ko/hisi_l0.ko，拷贝至环境目录，


```
insmod hisi_l0_mem_pool.ko
insmod hisi_l0.ko
```

驱动安装成功后，检查环境上/dev/l0设备是否生成

## 使用驱动
下载驱动的头文件hisi_l0_mem_pool.h与驱动符号文件Module.symvers
在待修改驱动中添加该头文件以及调用接口修改。驱动接口调用方法见API文档。
Makefile中需要导入符号文件
```
KBUILD_EXTRA_SYMBOLS += /path/to/mem_pol/Modle.symvers
```

## 使用L0内存库
将L0内存池库解压在/path/to/lib/路径下，内存池调用方法见API文档。

（1） 安装依赖包

```
yum install numactl
```

(2) 编译时，使用-L指定动态库路径，-I指定头文件路径


```
gcc -o program program.c -L/path/to/lib/ -I/path/to/lib/include -ll0mempool
```

(3)运行时，设置动态库的路径


```
export LD_LIBRARY_PATH=/path/to/lib/:$LD_LIBRARY_PATH
```
## 接口使用说明

- 内核态驱动等调用接口参考：https://gitee.com/cloudyyy1234/l0_doc/blob/master/l0_mem_pool.md
- 用户态直接申请内存参考：https://gitee.com/cloudyyy1234/l0_doc/blob/master/l0_mmap_api.md
- 用户态使用L0内存池库申请内存参考：https://gitee.com/cloudyyy1234/l0_doc/blob/master/l0_lib_api(2025).md


# 2024版本（调试版）
## 修改bios
在BIOS中设置cache mode为in shared out shared模式

## 安装内核

下载内核安装包：kernel-5.10.0_l0_mwp+-80.aarch64.rpm


```
rpm -ivh kernel-5.10.0_l0_mwp+-80.aarch64.rpm
```

## 安装驱动

```
modprobe hisi_l0.ko
```

驱动安装成功后，检查环境上/dev/l0设备是否生成

## 使用驱动
下载驱动的头文件hisi_l0_mem_pool.h与驱动符号文件Module.symvers
在待修改驱动中添加该头文件以及调用接口修改。驱动接口调用方法见API文档。
Makefile中需要导入符号文件
```
KBUILD_EXTRA_SYMBOLS += /path/to/mem_pol/Modle.symvers
```

## 使用L0内存库
将L0内存池库解压在/path/to/lib/路径下，内存池调用方法见API文档。

（1） 安装依赖包

```
yum install numactl
```

(2) 编译时，使用-L指定动态库路径，-I指定头文件路径


```
gcc -o program program.c -L/path/to/lib/ -I/path/to/lib/include -ll0mempool
```

(3)运行时，设置动态库的路径


```
export LD_LIBRARY_PATH=/path/to/lib/:$LD_LIBRARY_PATH
```


## 接口使用说明

- 用户态直接申请内存参考：l0_mmap_api.md
- 用户态使用L0内存池库申请内存参考：l0_lib_api(2025).md



