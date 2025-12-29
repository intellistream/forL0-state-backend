# Makefile for ForL0 Native Library
#
# This Makefile builds the JNI native library for L0 memory allocation.
# Supports macOS (dylib) and Linux (so).

# Detect OS
UNAME_S := $(shell uname -s)

# Java home detection
ifeq ($(JAVA_HOME),)
    ifeq ($(UNAME_S),Darwin)
        JAVA_HOME := $(shell /usr/libexec/java_home 2>/dev/null)
    else
        JAVA_HOME := $(shell dirname $(shell dirname $(shell readlink -f $(shell which java))))
    endif
endif

# Compiler settings
CC = gcc
CFLAGS = -fPIC -O2 -Wall

# Include paths for JNI headers
JNI_INCLUDE = -I"$(JAVA_HOME)/include"
ifeq ($(UNAME_S),Darwin)
    JNI_INCLUDE += -I"$(JAVA_HOME)/include/darwin"
    LIB_EXT = dylib
    LDFLAGS = -dynamiclib
else ifeq ($(UNAME_S),Linux)
    JNI_INCLUDE += -I"$(JAVA_HOME)/include/linux"
    LIB_EXT = so
    LDFLAGS = -shared
    # Link L0 memory pool library (check if exists)
    ifneq ($(wildcard /usr/lib64/libl0mempool.so),)
        LDFLAGS += -L/usr/lib64 -ll0mempool
        $(info Found libl0mempool.so - enabling L0 mode)
    else
        $(info libl0mempool.so not found - will use simulation mode)
    endif
else
    $(error Unsupported OS: $(UNAME_S))
endif

# Source and output
SRC = forl0_native.c
TARGET = libforl0_native.$(LIB_EXT)
OUTPUT_DIR = ../resources/native

.PHONY: all clean install info

all: $(TARGET)

$(TARGET): $(SRC)
	@echo "Building $(TARGET) for $(UNAME_S)..."
	@echo "JAVA_HOME: $(JAVA_HOME)"
	$(CC) $(CFLAGS) $(JNI_INCLUDE) $(LDFLAGS) -o $(TARGET) $(SRC)
	@echo "Built $(TARGET) successfully."

install: $(TARGET)
	@mkdir -p $(OUTPUT_DIR)
	@cp $(TARGET) $(OUTPUT_DIR)/
	@echo "Installed $(TARGET) to $(OUTPUT_DIR)/"

clean:
	@rm -f $(TARGET)
	@rm -f $(OUTPUT_DIR)/$(TARGET)
	@echo "Cleaned."

info:
	@echo "OS: $(UNAME_S)"
	@echo "JAVA_HOME: $(JAVA_HOME)"
	@echo "CC: $(CC)"
	@echo "CFLAGS: $(CFLAGS)"
	@echo "JNI_INCLUDE: $(JNI_INCLUDE)"
	@echo "LDFLAGS: $(LDFLAGS)"
	@echo "TARGET: $(TARGET)"
