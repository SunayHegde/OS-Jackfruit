CC      = gcc
CFLAGS  = -Wall -Wextra -O2 -pthread
KDIR   ?= /lib/modules/$(shell uname -r)/build

USER_TARGETS = engine cpu_hog memory_hog io_pulse

.PHONY: all clean load unload

all: $(USER_TARGETS) monitor.ko

engine: engine.c monitor_ioctl.h
	$(CC) $(CFLAGS) -o engine engine.c -pthread

cpu_hog: cpu_hog.c
	$(CC) $(CFLAGS) -o cpu_hog cpu_hog.c

memory_hog: memory_hog.c
	$(CC) $(CFLAGS) -o memory_hog memory_hog.c

io_pulse: io_pulse.c
	$(CC) $(CFLAGS) -o io_pulse io_pulse.c

obj-m := monitor.o

monitor.ko: monitor.c monitor_ioctl.h
	$(MAKE) -C $(KDIR) M=$(PWD) modules
	@echo "[OK] monitor.ko built"

load:
	sudo insmod monitor.ko
	@ls -l /dev/container_monitor

unload:
	sudo rmmod monitor || true

clean:
	$(MAKE) -C $(KDIR) M=$(PWD) clean
	rm -f $(USER_TARGETS) *.o
