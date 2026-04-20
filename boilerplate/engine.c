#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <time.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/ioctl.h>
#include <sched.h>
#include "monitor_ioctl.h"

#define MAX_CONTAINERS   32
#define LOG_DIR          "/tmp/engine_logs"
#define SOCK_PATH        "/tmp/engine.sock"
#define MONITOR_DEV      "/dev/container_monitor"
#define LOG_BUF_SIZE     256
#define LOG_ENTRY_MAX    1024
#define DEFAULT_SOFT_MB  64
#define DEFAULT_HARD_MB  128

typedef enum {
    STATE_STARTING = 0,
    STATE_RUNNING,
    STATE_STOPPED,
    STATE_KILLED,
    STATE_KILLED_OOM,
} ContainerState;

static const char *state_str(ContainerState s) {
    switch (s) {
        case STATE_STARTING:   return "starting";
        case STATE_RUNNING:    return "running";
        case STATE_STOPPED:    return "stopped";
        case STATE_KILLED:     return "killed";
        case STATE_KILLED_OOM: return "oom-killed";
        default:               return "unknown";
    }
}

typedef struct {
    int            used;
    char           name[64];
    pid_t          pid;
    time_t         start_time;
    ContainerState state;
    unsigned long  soft_limit_mb;
    unsigned long  hard_limit_mb;
    char           log_path[256];
    int            exit_status;
    int            pipe_read_fd;
    pthread_t      log_thread;
    int            log_thread_running;
} Container;

typedef struct {
    char data[LOG_ENTRY_MAX];
    int  len;
    char log_path[256];
} LogEntry;

static LogEntry        log_buffer[LOG_BUF_SIZE];
static int             log_head = 0;
static int             log_tail = 0;
static int             log_count = 0;
static pthread_mutex_t log_buf_mutex    = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  log_buf_not_empty = PTHREAD_COND_INITIALIZER;
static pthread_cond_t  log_buf_not_full  = PTHREAD_COND_INITIALIZER;
static volatile int    log_writer_running = 1;
static pthread_t       log_writer_thread;

static Container       containers[MAX_CONTAINERS];
static pthread_mutex_t containers_mutex = PTHREAD_MUTEX_INITIALIZER;
static volatile int    supervisor_running = 1;
static int             monitor_fd = -1;

static void log_push(const char *path, const char *data, int len)
{
    pthread_mutex_lock(&log_buf_mutex);
    while (log_count == LOG_BUF_SIZE)
        pthread_cond_wait(&log_buf_not_full, &log_buf_mutex);
    LogEntry *e = &log_buffer[log_tail];
    int copy = len < LOG_ENTRY_MAX ? len : LOG_ENTRY_MAX - 1;
    memcpy(e->data, data, copy);
    e->data[copy] = '\0';
    e->len = copy;
    strncpy(e->log_path, path, sizeof(e->log_path) - 1);
    log_tail = (log_tail + 1) % LOG_BUF_SIZE;
    log_count++;
    pthread_cond_signal(&log_buf_not_empty);
    pthread_mutex_unlock(&log_buf_mutex);
}

static void *log_writer(void *arg)
{
    (void)arg;
    while (log_writer_running || log_count > 0) {
        pthread_mutex_lock(&log_buf_mutex);
        while (log_count == 0 && log_writer_running)
            pthread_cond_wait(&log_buf_not_empty, &log_buf_mutex);
        if (log_count == 0) { pthread_mutex_unlock(&log_buf_mutex); break; }
        LogEntry e = log_buffer[log_head];
        log_head = (log_head + 1) % LOG_BUF_SIZE;
        log_count--;
        pthread_cond_signal(&log_buf_not_full);
        pthread_mutex_unlock(&log_buf_mutex);
        FILE *f = fopen(e.log_path, "a");
        if (f) { fwrite(e.data, 1, e.len, f); fclose(f); }
    }
    return NULL;
}

typedef struct { int fd; char path[256]; } ProducerArg;

static void *log_producer(void *arg)
{
    ProducerArg *pa = (ProducerArg *)arg;
    char buf[4096];
    ssize_t n;
    while ((n = read(pa->fd, buf, sizeof(buf) - 1)) > 0) {
        buf[n] = '\0';
        log_push(pa->path, buf, (int)n);
    }
    close(pa->fd);
    free(pa);
    return NULL;
}

static void monitor_register(pid_t pid, const char *name,
                              unsigned long soft_mb, unsigned long hard_mb)
{
    if (monitor_fd < 0) return;
    struct container_reg reg;
    memset(&reg, 0, sizeof(reg));
    reg.pid        = pid;
    reg.soft_limit = soft_mb * 1024 * 1024;
    reg.hard_limit = hard_mb * 1024 * 1024;
    strncpy(reg.name, name, sizeof(reg.name) - 1);
    ioctl(monitor_fd, MONITOR_REGISTER, &reg);
}

static void monitor_unregister(pid_t pid)
{
    if (monitor_fd < 0) return;
    struct container_unreg u;
    u.pid = pid;
    ioctl(monitor_fd, MONITOR_UNREGISTER, &u);
}

static Container *find_container(const char *name)
{
    for (int i = 0; i < MAX_CONTAINERS; i++)
        if (containers[i].used && strcmp(containers[i].name, name) == 0)
            return &containers[i];
    return NULL;
}

static Container *alloc_container(void)
{
    for (int i = 0; i < MAX_CONTAINERS; i++)
        if (!containers[i].used) return &containers[i];
    return NULL;
}

static void sigchld_handler(int sig)
{
    (void)sig;
    int status;
    pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&containers_mutex);
        for (int i = 0; i < MAX_CONTAINERS; i++) {
            if (containers[i].used && containers[i].pid == pid) {
                if (WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL)
                    containers[i].state = STATE_KILLED_OOM;
                else if (WIFSIGNALED(status))
                    containers[i].state = STATE_KILLED;
                else
                    containers[i].state = STATE_STOPPED;
                containers[i].exit_status = status;
                monitor_unregister(pid);
                break;
            }
        }
        pthread_mutex_unlock(&containers_mutex);
    }
}

static void sigterm_handler(int sig) { (void)sig; supervisor_running = 0; }

static void setup_container(const char *rootfs, const char *name)
{
    sethostname(name, strlen(name));
    mkdir("/proc", 0755);
    mount("proc", "/proc", "proc", 0, NULL);
    if (chroot(rootfs) < 0) { perror("chroot"); exit(1); }
    if (chdir("/") < 0) { perror("chdir /"); exit(1); }
    mount("proc", "/proc", "proc", 0, NULL);
}

static Container *do_start(const char *name, const char *rootfs,
                            char *const argv_cmd[],
                            unsigned long soft_mb, unsigned long hard_mb,
                            int foreground)
{
    pthread_mutex_lock(&containers_mutex);
    if (find_container(name)) {
        fprintf(stderr, "engine: container '%s' already exists\n", name);
        pthread_mutex_unlock(&containers_mutex);
        return NULL;
    }
    Container *c = alloc_container();
    if (!c) {
        fprintf(stderr, "engine: too many containers\n");
        pthread_mutex_unlock(&containers_mutex);
        return NULL;
    }
    memset(c, 0, sizeof(*c));
    c->used = 1;
    strncpy(c->name, name, sizeof(c->name) - 1);
    c->soft_limit_mb = soft_mb;
    c->hard_limit_mb = hard_mb;
    c->state = STATE_STARTING;
    c->start_time = time(NULL);
    snprintf(c->log_path, sizeof(c->log_path), "%s/%s.log", LOG_DIR, name);
    int pipefd[2];
    if (pipe2(pipefd, O_CLOEXEC) < 0) {
        perror("pipe2");
        c->used = 0;
        pthread_mutex_unlock(&containers_mutex);
        return NULL;
    }
    c->pipe_read_fd = pipefd[0];
    pthread_mutex_unlock(&containers_mutex);

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        close(pipefd[0]); close(pipefd[1]);
        pthread_mutex_lock(&containers_mutex);
        c->used = 0;
        pthread_mutex_unlock(&containers_mutex);
        return NULL;
    }
    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[1]);
        if (unshare(CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS) < 0) {
            perror("unshare"); exit(1);
        }
        setup_container(rootfs, name);
        execvp(argv_cmd[0], argv_cmd);
        perror("execvp");
        exit(1);
    }
    close(pipefd[1]);
    pthread_mutex_lock(&containers_mutex);
    c->pid   = pid;
    c->state = STATE_RUNNING;
    pthread_mutex_unlock(&containers_mutex);
    monitor_register(pid, name, soft_mb, hard_mb);
    ProducerArg *pa = malloc(sizeof(ProducerArg));
    pa->fd = pipefd[0];
    strncpy(pa->path, c->log_path, sizeof(pa->path) - 1);
    pthread_create(&c->log_thread, NULL, log_producer, pa);
    c->log_thread_running = 1;
    printf("engine: started container '%s' pid=%d\n", name, pid);
    if (foreground) {
        int status;
        waitpid(pid, &status, 0);
        pthread_mutex_lock(&containers_mutex);
        if (WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL)
            c->state = STATE_KILLED_OOM;
        else if (WIFSIGNALED(status))
            c->state = STATE_KILLED;
        else
            c->state = STATE_STOPPED;
        c->exit_status = status;
        pthread_mutex_unlock(&containers_mutex);
        monitor_unregister(pid);
        printf("engine: container '%s' exited\n", name);
    }
    return c;
}

static void cmd_ps(char *out, size_t outsz)
{
    char line[512];
    snprintf(out, outsz, "%-16s %-8s %-10s %-10s %-10s %-10s\n",
             "NAME", "PID", "STATE", "SOFT(MB)", "HARD(MB)", "STARTED");
    strncat(out, "--------------------------------------------------------------------\n",
            outsz - strlen(out) - 1);
    pthread_mutex_lock(&containers_mutex);
    for (int i = 0; i < MAX_CONTAINERS; i++) {
        if (!containers[i].used) continue;
        Container *c = &containers[i];
        char tsbuf[32];
        struct tm *tm_info = localtime(&c->start_time);
        strftime(tsbuf, sizeof(tsbuf), "%H:%M:%S", tm_info);
        snprintf(line, sizeof(line), "%-16s %-8d %-10s %-10lu %-10lu %-10s\n",
                 c->name, c->pid, state_str(c->state),
                 c->soft_limit_mb, c->hard_limit_mb, tsbuf);
        strncat(out, line, outsz - strlen(out) - 1);
    }
    pthread_mutex_unlock(&containers_mutex);
}

static void cmd_logs(const char *name, char *out, size_t outsz)
{
    pthread_mutex_lock(&containers_mutex);
    Container *c = find_container(name);
    if (!c) {
        snprintf(out, outsz, "error: container '%s' not found\n", name);
        pthread_mutex_unlock(&containers_mutex);
        return;
    }
    char path[256];
    strncpy(path, c->log_path, sizeof(path) - 1);
    pthread_mutex_unlock(&containers_mutex);
    FILE *f = fopen(path, "r");
    if (!f) { snprintf(out, outsz, "(no log yet)\n"); return; }
    size_t n = fread(out, 1, outsz - 1, f);
    out[n] = '\0';
    fclose(f);
    if (n == 0) snprintf(out, outsz, "(log is empty)\n");
}

static void cmd_stop(const char *name, char *out, size_t outsz)
{
    pthread_mutex_lock(&containers_mutex);
    Container *c = find_container(name);
    if (!c) {
        snprintf(out, outsz, "error: container '%s' not found\n", name);
        pthread_mutex_unlock(&containers_mutex);
        return;
    }
    if (c->state != STATE_RUNNING) {
        snprintf(out, outsz, "container '%s' is not running (state=%s)\n",
                 name, state_str(c->state));
        pthread_mutex_unlock(&containers_mutex);
        return;
    }
    pid_t pid = c->pid;
    pthread_mutex_unlock(&containers_mutex);
    kill(pid, SIGTERM);
    usleep(500000);
    pthread_mutex_lock(&containers_mutex);
    c = find_container(name);
    if (c && c->state == STATE_RUNNING) { kill(pid, SIGKILL); c->state = STATE_KILLED; }
    pthread_mutex_unlock(&containers_mutex);
    snprintf(out, outsz, "stopped container '%s'\n", name);
}

static void handle_client(int cfd, const char *rootfs)
{
    char req[1024] = {0};
    ssize_t n = recv(cfd, req, sizeof(req) - 1, 0);
    if (n <= 0) { close(cfd); return; }
    req[n] = '\0';
    char resp[65536] = {0};
    char cmd[64], arg1[256], arg2[512];
    int nargs = sscanf(req, "%63s %255s %511s", cmd, arg1, arg2);
    (void)nargs;
    if (strcmp(cmd, "ps") == 0) {
        cmd_ps(resp, sizeof(resp));
    } else if (strcmp(cmd, "logs") == 0) {
        cmd_logs(arg1, resp, sizeof(resp));
    } else if (strcmp(cmd, "stop") == 0) {
        cmd_stop(arg1, resp, sizeof(resp));
    } else if (strcmp(cmd, "start") == 0 || strcmp(cmd, "run") == 0) {
        char rest[512] = {0};
        sscanf(req, "%*s %255s %511[^\n]", arg1, rest);
        char *tokens[64];
        int tc = 0;
        char *tok = strtok(rest, " ");
        while (tok && tc < 63) { tokens[tc++] = tok; tok = strtok(NULL, " "); }
        tokens[tc] = NULL;
        if (tc == 0) { tokens[0] = "/bin/sh"; tokens[1] = NULL; tc = 1; }
        int fg = (strcmp(cmd, "run") == 0);
        Container *c = do_start(arg1, rootfs, tokens, DEFAULT_SOFT_MB, DEFAULT_HARD_MB, fg);
        if (c) snprintf(resp, sizeof(resp), "started '%s' pid=%d\n", arg1, c->pid);
        else   snprintf(resp, sizeof(resp), "error: failed to start '%s'\n", arg1);
    } else {
        snprintf(resp, sizeof(resp), "unknown command: %s\n", cmd);
    }
    send(cfd, resp, strlen(resp), 0);
    close(cfd);
}

static void run_supervisor(const char *rootfs)
{
    struct sigaction sa_chld = {0};
    sa_chld.sa_handler = sigchld_handler;
    sa_chld.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa_chld, NULL);
    struct sigaction sa_term = {0};
    sa_term.sa_handler = sigterm_handler;
    sigaction(SIGTERM, &sa_term, NULL);
    sigaction(SIGINT,  &sa_term, NULL);
    mkdir(LOG_DIR, 0777);
    monitor_fd = open(MONITOR_DEV, O_RDWR);
    if (monitor_fd < 0)
        fprintf(stderr, "engine: warning: cannot open %s -- memory limits disabled\n", MONITOR_DEV);
    pthread_create(&log_writer_thread, NULL, log_writer, NULL);
    unlink(SOCK_PATH);
    int sfd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sfd < 0) { perror("socket"); exit(1); }
    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCK_PATH, sizeof(addr.sun_path) - 1);
    if (bind(sfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) { perror("bind"); exit(1); }
    if (listen(sfd, 16) < 0) { perror("listen"); exit(1); }
    chmod(SOCK_PATH, 0777);
    fcntl(sfd, F_SETFL, O_NONBLOCK);
    printf("engine: supervisor started, rootfs=%s socket=%s\n", rootfs, SOCK_PATH);
    fflush(stdout);
    while (supervisor_running) {
        int cfd = accept(sfd, NULL, NULL);
        if (cfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) { usleep(100000); continue; }
            if (errno == EINTR) continue;
            break;
        }
        handle_client(cfd, rootfs);
    }
    printf("engine: supervisor shutting down...\n");
    pthread_mutex_lock(&containers_mutex);
    for (int i = 0; i < MAX_CONTAINERS; i++)
        if (containers[i].used && containers[i].state == STATE_RUNNING)
            kill(containers[i].pid, SIGTERM);
    pthread_mutex_unlock(&containers_mutex);
    sleep(1);
    while (waitpid(-1, NULL, WNOHANG) > 0);
    log_writer_running = 0;
    pthread_cond_broadcast(&log_buf_not_empty);
    pthread_join(log_writer_thread, NULL);
    close(sfd);
    unlink(SOCK_PATH);
    if (monitor_fd >= 0) close(monitor_fd);
    printf("engine: supervisor exited cleanly\n");
}

static void send_command(const char *cmd)
{
    int sfd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sfd < 0) { perror("socket"); exit(1); }
    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCK_PATH, sizeof(addr.sun_path) - 1);
    if (connect(sfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "engine: cannot connect to supervisor at %s\n"
                        "        Is it running? Try: sudo ./engine supervisor <rootfs>\n", SOCK_PATH);
        close(sfd); exit(1);
    }
    send(sfd, cmd, strlen(cmd), 0);
    shutdown(sfd, SHUT_WR);
    char buf[65536];
    ssize_t n;
    while ((n = recv(sfd, buf, sizeof(buf) - 1, 0)) > 0) {
        buf[n] = '\0'; printf("%s", buf);
    }
    close(sfd);
}

static void usage(const char *prog)
{
    fprintf(stderr,
        "Usage:\n"
        "  %s supervisor <rootfs>           # start supervisor\n"
        "  %s start <name> [cmd [args...]]  # launch container (background)\n"
        "  %s run   <name> [cmd [args...]]  # launch container (foreground)\n"
        "  %s ps                            # list containers\n"
        "  %s logs  <name>                  # show logs\n"
        "  %s stop  <name>                  # stop container\n",
        prog, prog, prog, prog, prog, prog);
    exit(1);
}

int main(int argc, char *argv[])
{
    if (argc < 2) usage(argv[0]);
    const char *cmd = argv[1];
    if (strcmp(cmd, "supervisor") == 0) {
        if (argc < 3) usage(argv[0]);
        run_supervisor(argv[2]);
    } else if (strcmp(cmd, "start") == 0 || strcmp(cmd, "run") == 0) {
        if (argc < 3) usage(argv[0]);
        char req[1024];
        int off = snprintf(req, sizeof(req), "%s %s", cmd, argv[2]);
        for (int i = 3; i < argc && off < (int)sizeof(req) - 2; i++)
            off += snprintf(req + off, sizeof(req) - off, " %s", argv[i]);
        send_command(req);
    } else if (strcmp(cmd, "ps") == 0) {
        send_command("ps");
    } else if (strcmp(cmd, "logs") == 0) {
        if (argc < 3) usage(argv[0]);
        char req[256];
        snprintf(req, sizeof(req), "logs %s", argv[2]);
        send_command(req);
    } else if (strcmp(cmd, "stop") == 0) {
        if (argc < 3) usage(argv[0]);
        char req[256];
        snprintf(req, sizeof(req), "stop %s", argv[2]);
        send_command(req);
    } else {
        usage(argv[0]);
    }
    return 0;
}
