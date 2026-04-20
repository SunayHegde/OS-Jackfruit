#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/init.h>
#include <linux/fs.h>
#include <linux/miscdevice.h>
#include <linux/uaccess.h>
#include <linux/slab.h>
#include <linux/list.h>
#include <linux/mutex.h>
#include <linux/timer.h>
#include <linux/sched.h>
#include <linux/sched/signal.h>
#include <linux/mm.h>
#include <linux/pid.h>
#include <linux/jiffies.h>
#include "monitor_ioctl.h"

MODULE_LICENSE("GPL");
MODULE_AUTHOR("OS-Jackfruit Team");
MODULE_DESCRIPTION("Container memory monitor LKM");
MODULE_VERSION("1.0");

#define CHECK_INTERVAL_MS 2000
#define DEVICE_NAME "container_monitor"

struct container_entry {
    pid_t pid;
    unsigned long soft_limit;
    unsigned long hard_limit;
    char name[64];
    bool soft_warned;
    struct list_head list;
};

static LIST_HEAD(container_list);
static DEFINE_MUTEX(list_mutex);
static struct timer_list check_timer;

static unsigned long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    unsigned long rss = 0;
    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task && task->mm)
        rss = get_mm_rss(task->mm) << PAGE_SHIFT;
    rcu_read_unlock();
    return rss;
}

static bool pid_alive_check(pid_t pid)
{
    struct task_struct *task;
    bool alive = false;
    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task && !task->exit_state)
        alive = true;
    rcu_read_unlock();
    return alive;
}

static int send_signal_to_pid(pid_t pid, int sig)
{
    struct task_struct *task;
    int ret = -ESRCH;
    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task)
        ret = send_sig(sig, task, 1);
    rcu_read_unlock();
    return ret;
}

static void check_containers(struct timer_list *t)
{
    struct container_entry *entry, *tmp;
    mutex_lock(&list_mutex);
    list_for_each_entry_safe(entry, tmp, &container_list, list) {
        if (!pid_alive_check(entry->pid)) {
            pr_info("container_monitor: container '%s' (pid %d) no longer alive, removing\n",
                    entry->name, entry->pid);
            list_del(&entry->list);
            kfree(entry);
            continue;
        }
        unsigned long rss = get_rss_bytes(entry->pid);
        if (entry->hard_limit > 0 && rss >= entry->hard_limit) {
            pr_warn("container_monitor: [HARD LIMIT] container '%s' (pid %d) RSS=%lu KB >= hard_limit=%lu KB -- sending SIGKILL\n",
                    entry->name, entry->pid, rss / 1024, entry->hard_limit / 1024);
            send_signal_to_pid(entry->pid, SIGKILL);
            list_del(&entry->list);
            kfree(entry);
            continue;
        }
        if (entry->soft_limit > 0 && rss >= entry->soft_limit && !entry->soft_warned) {
            pr_warn("container_monitor: [SOFT LIMIT] container '%s' (pid %d) RSS=%lu KB >= soft_limit=%lu KB -- warning\n",
                    entry->name, entry->pid, rss / 1024, entry->soft_limit / 1024);
            entry->soft_warned = true;
        }
    }
    mutex_unlock(&list_mutex);
    mod_timer(&check_timer, jiffies + msecs_to_jiffies(CHECK_INTERVAL_MS));
}

static long monitor_ioctl(struct file *file, unsigned int cmd, unsigned long arg)
{
    switch (cmd) {
    case MONITOR_REGISTER: {
        struct container_reg reg;
        struct container_entry *entry;
        if (copy_from_user(&reg, (void __user *)arg, sizeof(reg)))
            return -EFAULT;
        entry = kzalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry)
            return -ENOMEM;
        entry->pid        = reg.pid;
        entry->soft_limit = reg.soft_limit;
        entry->hard_limit = reg.hard_limit;
        entry->soft_warned = false;
        strncpy(entry->name, reg.name, sizeof(entry->name) - 1);
        INIT_LIST_HEAD(&entry->list);
        mutex_lock(&list_mutex);
        list_add_tail(&entry->list, &container_list);
        mutex_unlock(&list_mutex);
        pr_info("container_monitor: registered '%s' pid=%d soft=%lu KB hard=%lu KB\n",
                entry->name, entry->pid,
                entry->soft_limit / 1024, entry->hard_limit / 1024);
        return 0;
    }
    case MONITOR_UNREGISTER: {
        struct container_unreg unreg;
        struct container_entry *entry, *tmp;
        if (copy_from_user(&unreg, (void __user *)arg, sizeof(unreg)))
            return -EFAULT;
        mutex_lock(&list_mutex);
        list_for_each_entry_safe(entry, tmp, &container_list, list) {
            if (entry->pid == unreg.pid) {
                pr_info("container_monitor: unregistered '%s' pid=%d\n",
                        entry->name, entry->pid);
                list_del(&entry->list);
                kfree(entry);
                break;
            }
        }
        mutex_unlock(&list_mutex);
        return 0;
    }
    case MONITOR_LIST: {
        struct container_entry *entry;
        pr_info("container_monitor: === tracked containers ===\n");
        mutex_lock(&list_mutex);
        list_for_each_entry(entry, &container_list, list) {
            unsigned long rss = get_rss_bytes(entry->pid);
            pr_info("  '%s' pid=%d rss=%lu KB soft=%lu KB hard=%lu KB\n",
                    entry->name, entry->pid,
                    rss / 1024, entry->soft_limit / 1024, entry->hard_limit / 1024);
        }
        mutex_unlock(&list_mutex);
        return 0;
    }
    default:
        return -ENOTTY;
    }
}

static const struct file_operations monitor_fops = {
    .owner          = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

static struct miscdevice monitor_dev = {
    .minor = MISC_DYNAMIC_MINOR,
    .name  = DEVICE_NAME,
    .fops  = &monitor_fops,
    .mode  = 0666,
};

static int __init monitor_init(void)
{
    int ret;
    ret = misc_register(&monitor_dev);
    if (ret) {
        pr_err("container_monitor: failed to register device (%d)\n", ret);
        return ret;
    }
    timer_setup(&check_timer, check_containers, 0);
    mod_timer(&check_timer, jiffies + msecs_to_jiffies(CHECK_INTERVAL_MS));
    pr_info("container_monitor: loaded, device=/dev/%s\n", DEVICE_NAME);
    return 0;
}

static void __exit monitor_exit(void)
{
    struct container_entry *entry, *tmp;
    del_timer_sync(&check_timer);
    mutex_lock(&list_mutex);
    list_for_each_entry_safe(entry, tmp, &container_list, list) {
        list_del(&entry->list);
        kfree(entry);
    }
    mutex_unlock(&list_mutex);
    misc_deregister(&monitor_dev);
    pr_info("container_monitor: unloaded\n");
}

module_init(monitor_init);
module_exit(monitor_exit);
