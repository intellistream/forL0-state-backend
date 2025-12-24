# mmap/unmmap 系统调用 API 文档

## 1. mmap 系统调用

### 函数原型
```c
#include <sys/mman.h>
void* mmap(void* addr, size_t length, int prot, int flags, int fd, off_t offset);
```

### 参数说明
| 参数 | 类型 | 说明 |
|------|------|------|
| addr | void* | 指定映射区域在进程地址空间中的期望起始地址，通常设为 NULL |
| length | size_t | 映射区域的长度（字节数） |
| prot | int | 内存保护标志，指定映射区域的访问权限（可读/可写/可执行） |
| flags | int | 映射类型标志（共享/私有/匿名等） |
| fd | int | 文件描述符，指向要映射的文件 |
| offset | off_t | 文件偏移量，必须是 4096 的整数倍 |

### 返回值
- 成功：返回映射区的内存起始地址
- 失败：返回 MAP_FAILED（即 (void*)-1），错误原因存于 errno 中

## 2. munmap 系统调用

### 函数原型
```c
#include <sys/mman.h>
int munmap(void* addr, size_t length);
```

### 参数说明
| 参数 | 类型 | 说明 |
|------|------|------|
| addr | void* | mmap 返回的映射区起始地址 |
| length | size_t | 要解除映射的区域长度 |

### 返回值
- 成功：返回 0
- 失败：返回 -1，错误原因存于 errno 中

### 使用示例
```c
#include <sys/mman.h>
#include <numaif.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>


#define DEV     "/dev/hisi_l0"

void *mmap_alloc(unsigned long size, int fd)
{
    void *addr;
    void *align_addr;
    unsigned long align = 2097152;

    printf("size %lu\n", size);

    int n = 5;
    while (n > 0) {
        addr =
            (unsigned long long)mmap(NULL, size + 2097152, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (addr == MAP_FAILED) {
            printf(" 1 mmap failed\n");
            return NULL;
        }
        printf("addr: %#llx\n", (unsigned long long)addr);
        if ((unsigned long)addr % align == 0) {
            align_addr = addr;
        } else {
            align_addr = addr + align - ((unsigned long)addr % align);
        }
        printf("align addr: %#llx\n", (unsigned long long)align_addr);

        if (munmap(addr, size + 2097152) == -1) {
            return NULL;
        }

        addr = (void *)mmap(align_addr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (addr == MAP_FAILED) {
            printf("2 mmap failed, error %d\n", errno);
            // return -1;

            if (errno == 22) {
                n--;
                printf("3 mmap failed, retry %d\n", n);
                continue;
            } else {
                return NULL;
            }
        } else {
            break;
        }
    }

    if (n == 0) {
        return NULL;
    }

    memset(addr, 1, size);

    if (munmap(addr, size) == -1) {
        printf("munmap failed.\n");
        return -1;
    }
    return addr;
}

int main()
{
    int fd;
    fd = open(DEV, O_RDWR, 0777);
    if (fd < 0) {
        printf("open fd failed.\n");
        return -1;
    }
    mmap_alloc(48 * 1024 * 1024, fd);
    close(fd);
    fd = 0;
    return 0;
}
```

