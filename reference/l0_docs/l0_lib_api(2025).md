# L0内存池库API使用说明文档

## 概述

L0内存池库是一个针对性能优化和NUMA感知分配的内存管理系统。它支持通用内存池和原始内存池，并且可以通过L0内存设备进行分配策略优化。

## 核心组件

### 1. 缓存调节器(Cache Tuner)

缓存调节器是系统中内存池管理的核心组件。

#### 函数接口

- `int cache_tuner_init(cache_tuner **tuner, size_t max_capacity)`
  - 初始化缓存调节器，设置最大容量
  - 参数:
    - `tuner`: 指向缓存调节器指针的指针
    - `max_capacity`: 缓存可保留的最大容量
  - 返回值: 成功返回 RET_SUCCESS，否则返回错误码

- `void cache_tuner_dump_status(cache_tuner *tuner, int detail)`
  - 将内存池状态输出到日志
  - 参数:
    - `tuner`: 缓存调节器
    - `detail`: 日志详细程度(0-2)

- `int cache_tuner_destroy(cache_tuner *tuner)`
  - 销毁缓存调节器并释放所有内存池
  - 参数:
    - `tuner`: 要销毁的缓存调节器
  - 返回值: 成功返回 RET_SUCCESS，否则返回错误码

### 2. 内存池管理

#### 通用内存池

- `mem_pool *mem_pool_create(cache_tuner *tuner, const char *name, size_t init_size, size_t inc_size)`
  - 创建一个新的内存池
  - 参数:
    - `tuner`: 缓存调节器
    - `name`: 内存池名称(最长31个字符)
    - `init_size`: 内存池初始大小
    - `inc_size`: 内存池扩展时的增量大小
  - 返回值: 指向创建的内存池的指针

- `void mem_pool_release(mem_pool **ppool)`
  - 将内存池释放回工厂
  - 参数:
    - `ppool`: 指向内存池指针的指针

- `void mem_pool_release_and_clear(mem_pool **ppool)`
  - 释放内存池并清除其内容
  - 参数:
    - `ppool`: 指向内存池指针的指针

#### 原始内存池

- `void *mem_pool_create_raw(cache_tuner *tuner, const char *name, size_t size)`
  - 创建一个新的原始内存池
  - 参数:
    - `tuner`: 缓存调节器
    - `name`: 内存池名称(最长31个字符)
    - `size`: 原始内存池大小
  - 返回值: 指向原始内存的指针

- `void mem_pool_release_raw(void **ppool)`
  - 释放原始内存池
  - 参数:
    - `ppool`: 指向原始内存池指针的指针

- `void mem_pool_release_and_clear_raw(void **ppool)`
  - 释放原始内存池并清除其内容
  - 参数:
    - `ppool`: 指向原始内存池指针的指针

### 3. 内存分配

#### 通用内存池分配

- `void *mem_pool_alloc(mem_pool *pool, size_t size)`
  - 从内存池中分配内存
  - 参数:
    - `pool`: 内存池
    - `size`: 要分配的大小
  - 返回值: 指向已分配内存的指针

- `void *mem_pool_alloc_safe(mem_pool *pool, size_t size)`
  - 线程安全的内存池分配
  - 参数:
    - `pool`: 内存池
    - `size`: 要分配的大小
  - 返回值: 指向已分配内存的指针

- `void *mem_pool_calloc(mem_pool *pool, size_t count, size_t elem)`
  - 分配并初始化为零的内存
  - 参数:
    - `pool`: 内存池
    - `count`: 元素数量
    - `elem`: 每个元素的大小
  - 返回值: 指向已分配并初始化为零的内存的指针

- `void *mem_pool_calloc_safe(mem_pool *pool, size_t count, size_t elem)`
  - 线程安全的分配并初始化为零的内存
  - 参数:
    - `pool`: 内存池
    - `count`: 元素数量
    - `elem`: 每个元素的大小
  - 返回值: 指向已分配并初始化为零的内存的指针

- `void *mem_pool_zalloc(mem_pool *pool, size_t size)`
  - 分配并初始化为零的内存(单个元素)
  - 参数:
    - `pool`: 内存池
    - `size`: 要分配的大小
  - 返回值: 指向已分配并初始化为零的内存的指针

- `void *mem_pool_zalloc_safe(mem_pool *pool, size_t size)`
  - 线程安全的分配并初始化为零的内存(单个元素)
  - 参数:
    - `pool`: 内存池
    - `size`: 要分配的大小
  - 返回值: 指向已分配并初始化为零的内存的指针

### 4. 内存池信息

- `const char *mem_pool_getname(const mem_pool *pool, const int pool_type)`
  - 获取内存池名称
  - 参数:
    - `pool`: 内存池
    - `pool_type`: 内存池类型(GENERAL_POOL 或 RAW_POOL)
  - 返回值: 以null结尾的字符串形式的内存池名称

- `size_t mem_pool_get_capacity(const mem_pool *pool, const int pool_type)`
  - 获取内存池总容量
  - 参数:
    - `pool`: 内存池
    - `pool_type`: 内存池类型(GENERAL_POOL 或 RAW_POOL)
  - 返回值: 总容量(字节)

- `size_t mem_pool_get_used_size(const mem_pool *pool, const int pool_type)`
  - 获取内存池已使用大小
  - 参数:
    - `pool`: 内存池
    - `pool_type`: 内存池类型(GENERAL_POOL 或 RAW_POOL)
  - 返回值: 已使用大小(字节)

### 5. 直接内存分配 (当启用DIRECTLY_ALLOC_FREE时)

- `void *l0_mem_alloc(cache_tuner *tuner, size_t size)`
  - 直接从L0设备分配内存
  - 参数:
    - `tuner`: 缓存调节器
    - `size`: 要分配的大小
  - 返回值: 指向已分配内存的指针

- `int l0_mem_free(cache_tuner *tuner, void *p)`
  - 释放直接分配的内存
  - 参数:
    - `tuner`: 缓存调节器
    - `p`: 要释放的内存指针
  - 返回值: 成功返回 RET_SUCCESS，否则返回错误码

### 6. 日志

- `void log_set_level(int level)`
  - 设置最大日志级别
  - 参数:
    - `level`: 日志级别(NO_LOG, LOG_ERROR, LOG_INFO, LOG_DEBUG)

- `void log_set_quiet(bool enable)`
  - 启用或禁用静默模式(不输出到stdout)
  - 参数:
    - `enable`: 为true时启用静默模式，否则禁用

## 常量定义

- 内存池类型:
  - `GENERAL_POOL`: 通用内存池
  - `RAW_POOL`: 原始内存池

- 返回值:
  - `RET_SUCCESS`: 操作成功
  - `RET_FAIL`: 操作失败
  - `RET_NOMEM_ERR`: 内存不足
  - `RET_INVALID_PARAM`: 参数无效

- 日志级别:
  - `NO_LOG`: 无日志
  - `LOG_ERROR`: 错误级别日志
  - `LOG_INFO`: 信息级别日志
  - `LOG_DEBUG`: 调试级别日志

## 使用示例

### 通过内存池申请内存示例

创建mem_pool，然后从pool中申请内存
```c
#include "l0_mem_pool.h"

int main() {
    cache_tuner *tuner;
    
    // 初始化缓存调节器，最大容量为30MB
    if (cache_tuner_init(&tuner, 30 * 1024 * 1024) != RET_SUCCESS) {
        return -1;
    }
    
    // 创建内存池
    mem_pool *pool = mem_pool_create(tuner, "example_pool", 
                                     1024 * 1024, 512 * 1024);
    if (!pool) {
        cache_tuner_destroy(tuner);
        return -1;
    }
    
    // 从内存池分配内存
    void *memory = mem_pool_alloc(pool, 1024);
    if (!memory) {
        mem_pool_release(&pool);
        cache_tuner_destroy(tuner);
        return -1;
    }
    
    // 使用分配的内存...
    
    // 清理资源
    mem_pool_release(&pool);
    cache_tuner_destroy(tuner);
    
    return 0;
}
```

### 申请内存池，自己管理内存
创建raw_pool, pool中只有一片内存，不支持alloc，需要自己管理

```c

#include <sys/time.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "l0_mem_pool.h"
#include "log.h"

cache_tuner *tuner;

static int test_pool()
{
    // 创建透传类型内存池
    log_info("***********test_pool1 create test_pool1, init size 2M");
    void *pool = mem_pool_create_raw(tuner, "test_pool1", 2*1024*1024);
    if (!pool)
        return -1;

    // 其他内存操作...

    // 打印内存池的名称、容量、已使用的大小
    log_info("pool %16s pool capacity %u, used_size %ld",
        mem_pool_getname(pool, 1),
        mem_pool_get_capacity(pool, 1),
        mem_pool_get_used_size(pool, 1));

    memset(pool, 0, 1024);

    // 详细打印缓存池管理器当前的内存占用情况
    cache_tuner_dump_status(tuner, 2);

    // 释放内存池3
    log_info("***********test_pool1 release test_pool1, init size 2M");
    mem_pool_release_raw((mem_pool**)&pool);

    return 0;
}

int main(int argc, char *argv[])
{
    // 设置日志打印等级
    log_set_level(LOG_INFO);

    // 创建缓存池管理器，限制最大的容量为20M
    if (cache_tuner_init(&tuner, 70 * 1024 * 1024) != 0)
    {
        log_error("cache_tuner_init failed.");
        return 1;
    }

    test_pool();

    cache_tuner_dump_status(tuner, 2);

    // 释放缓存池管理器
    if (cache_tuner_destroy(tuner) != 0)
    {
        log_error("cache_tuner_init failed.");
    }
    return 0;
}
```

### 直接申请内存
不关心pool的使用情况，直接申请内存。
```c


static int test_pool()
{
    log_info("***********test_pool_mix malloc");

    void *mp[30];
    int i;
    int test_count = 5;
    for (i = 0; i < test_count; i++) {
        log_info("---------------------------------test_pool_mix alloc %d-------------------------------------", i);

        if (i % 2 == 0) {
            mp[i] = l0_mem_alloc(tuner, 512);

        } else {
            mp[i] = l0_mem_alloc(tuner, i * 1024 * 1024);
            log_info("mp %d is %lx", i, (long)mp[i]);
        }

        if (i % 2 == 0) {
            log_info("---------------------------------test_pool_mix free %d-------------------------------------", i);
            l0_mem_free(tuner, mp[i]);

        } else {

            log_info("---------------------------------test_pool_mix free %d-------------------------------------", i);
            l0_mem_free(tuner, mp[i - 1]);
        }
        cache_tuner_dump_status(tuner, 2);
    }

    return 0;
}

int main(int argc, char *argv[])
{
    log_set_level(LOG_DEBUG);

    if (cache_tuner_init(&tuner, 160 * 1024 * 1024) != 0) {
        log_error("cache_tuner_init failed.");
        return 1;
    }

    test_pool();

    if (cache_tuner_destroy(tuner) != 0) {
        log_error("cache_tuner_init failed.");
    }
    return 0;
}
```
