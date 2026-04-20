#ifndef MONITOR_IOCTL_H
#define MONITOR_IOCTL_H

#include <linux/ioctl.h>

#define MONITOR_MAGIC 'M'

struct container_reg {
    pid_t pid;
    unsigned long soft_limit;
    unsigned long hard_limit;
    char name[64];
};

struct container_unreg {
    pid_t pid;
};

#define MONITOR_REGISTER   _IOW(MONITOR_MAGIC, 1, struct container_reg)
#define MONITOR_UNREGISTER _IOW(MONITOR_MAGIC, 2, struct container_unreg)
#define MONITOR_LIST       _IO(MONITOR_MAGIC,  3)

#endif
