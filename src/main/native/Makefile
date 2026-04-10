################################################################################
#  ForL0 Native Engine - Makefile (无需 cmake)
#
#  用法:
#    make                 # 编译 Release
#    make DEBUG=1         # 编译 Debug (带 sanitizer)
#    make install         # 复制到 resources/native/
#    make clean           # 清理
################################################################################

# --------------------------------------------------------------------------
# JNI paths (auto-detect JAVA_HOME)
# --------------------------------------------------------------------------
ifeq ($(strip $(JAVA_HOME)),)
    # Method 1: java -XshowSettings (returns JRE path on JDK8, e.g. /usr/lib/jvm/java-8/jre)
    JAVA_HOME := $(shell java -XshowSettings:property -version 2>&1 | grep 'java.home' | awk -F'= ' '{print $$2}' | sed 's|/jre$$||')
    ifeq ($(strip $(JAVA_HOME)),)
        # Method 2: resolve symlinks from $(which java)
        JAVA_HOME := $(shell readlink -f $$(which java) 2>/dev/null | sed -e 's|/jre/bin/java$$||' -e 's|/bin/java$$||')
    endif
    ifeq ($(strip $(JAVA_HOME)),)
        $(error JAVA_HOME is not set and could not be auto-detected. Please set it: make JAVA_HOME=/path/to/jdk)
    endif
endif

# Find jni.h: check JAVA_HOME/include first, then JAVA_HOME/../include (JRE layout)
JNI_INCLUDE := $(JAVA_HOME)/include
ifeq ($(wildcard $(JNI_INCLUDE)/jni.h),)
    JNI_INCLUDE := $(JAVA_HOME)/../include
endif
ifeq ($(wildcard $(JNI_INCLUDE)/jni.h),)
    $(error jni.h not found. JAVA_HOME=$(JAVA_HOME), tried $(JAVA_HOME)/include and $(JAVA_HOME)/../include. Is a full JDK (not JRE) installed?)
endif
JNI_INCLUDE_OS := $(wildcard $(JNI_INCLUDE)/linux $(JNI_INCLUDE)/darwin)

$(info JAVA_HOME     = $(JAVA_HOME))
$(info JNI_INCLUDE   = $(JNI_INCLUDE))
$(info JNI_INCLUDE_OS= $(JNI_INCLUDE_OS))

# --------------------------------------------------------------------------
# Compiler settings
# --------------------------------------------------------------------------
CXX      := g++
CXXFLAGS := -std=c++17 -fPIC -Wall -Wextra -Wno-unused-parameter
LDFLAGS  := -shared -ldl

INCLUDES := -I. -Iengine -Iplatform -Icheckpoint -Ijni \
            -I$(JNI_INCLUDE) $(addprefix -I,$(JNI_INCLUDE_OS))

# --------------------------------------------------------------------------
# Platform / SIMD detection
# --------------------------------------------------------------------------
ARCH := $(shell uname -m)
ifeq ($(ARCH),aarch64)
    CXXFLAGS += -DFORL0_NEON=1
else ifeq ($(ARCH),x86_64)
    CXXFLAGS += -DFORL0_SSE2=1
    # Check AVX2 support
    ifneq ($(shell $(CXX) -mavx2 -E -x c /dev/null 2>/dev/null && echo yes),)
        CXXFLAGS += -DFORL0_HAS_AVX2=1
    endif
else
    CXXFLAGS += -DFORL0_PORTABLE=1
endif

# --------------------------------------------------------------------------
# Build mode
# --------------------------------------------------------------------------
ifdef DEBUG
    CXXFLAGS += -O0 -g -fsanitize=address,undefined
    LDFLAGS  += -fsanitize=address,undefined
else
    CXXFLAGS += -O3 -DNDEBUG
    ifneq ($(shell uname),Darwin)
        CXXFLAGS += -march=native
    endif
endif

# --------------------------------------------------------------------------
# Sources and target
# --------------------------------------------------------------------------
SOURCES := jni/forl0_jni.cpp \
           jni/jni_value_state.cpp \
           jni/jni_list_state.cpp \
           jni/jni_map_state.cpp \
           jni/jni_tw_state.cpp \
           jni/jni_checkpoint.cpp

OBJECTS := $(SOURCES:.cpp=.o)
TARGET  := libforl0_engine.so
NATIVE_RESOURCE_DIR := ../resources/native

# --------------------------------------------------------------------------
# Rules
# --------------------------------------------------------------------------
.PHONY: all clean install

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CXX) $(LDFLAGS) -o $@ $^
	@echo ""
	@echo "=== Build complete: $(TARGET) ==="

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c -o $@ $<

install: $(TARGET)
	@mkdir -p $(NATIVE_RESOURCE_DIR)
	cp $(TARGET) $(NATIVE_RESOURCE_DIR)/
	@echo "Installed to $(NATIVE_RESOURCE_DIR)/$(TARGET)"

clean:
	rm -f $(OBJECTS) $(TARGET)
