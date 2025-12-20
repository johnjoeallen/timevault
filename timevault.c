#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <limits.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/mount.h>
#include <time.h>
#include <unistd.h>
#include <dirent.h>
#include <yaml.h>
#include <signal.h>

#define LOCK_FILE "/var/run/timevault.pid"
#define DEFAULT_CONFIG "/etc/timevault.yaml"
#define TIMEVAULT_MARKER ".timevault"
#define TIMEVAULT_VERSION "0.1.0"
#define TIMEVAULT_LICENSE "GNU GPL v3 or later"
#define TIMEVAULT_COPYRIGHT "Copyright (C) 2025 John Allen (john.joe.alleN@gmail.com)"
#define TIMEVAULT_PROJECT_URL "https://github.com/johnjoeallen/timevault"

static char **tracked_mounts = NULL;
static size_t tracked_mounts_count = 0;
static size_t tracked_mounts_cap = 0;

struct RunMode {
    int dry_run;
    int safe_mode;
    int verbose;
};

typedef enum {
    RUN_AUTO,
    RUN_DEMAND,
    RUN_OFF
} RunPolicy;

struct Job {
    char *name;
    char *source;
    char *dest;
    int copies;
    char *mount;
    RunPolicy run_policy;
    char **excludes;
    size_t excludes_count;
    char **depends_on;
    size_t depends_on_count;
};

struct Config {
    struct Job *jobs;
    size_t jobs_count;
    char **excludes;
    size_t excludes_count;
    char *mount_prefix;
};

static void free_job(struct Job *job) {
    size_t i;
    if (!job) return;
    free(job->name);
    free(job->source);
    free(job->dest);
    free(job->mount);
    for (i = 0; i < job->excludes_count; i++) {
        free(job->excludes[i]);
    }
    free(job->excludes);
    for (i = 0; i < job->depends_on_count; i++) {
        free(job->depends_on[i]);
    }
    free(job->depends_on);
}

static void free_config(struct Config *cfg) {
    size_t i;
    if (!cfg) return;
    for (i = 0; i < cfg->jobs_count; i++) {
        free_job(&cfg->jobs[i]);
    }
    free(cfg->jobs);
    for (i = 0; i < cfg->excludes_count; i++) {
        free(cfg->excludes[i]);
    }
    free(cfg->excludes);
    free(cfg->mount_prefix);
}

static void print_command(char *const argv[], struct RunMode mode) {
    int i = 0;
    if (!mode.dry_run && !mode.verbose) return;
    for (i = 0; argv[i]; i++) {
        if (i == 0) {
            printf("%s", argv[i]);
        } else {
            printf(" %s", argv[i]);
        }
    }
    printf("\n");
}

static int run_command(char *const argv[], struct RunMode mode) {
    pid_t pid;
    int status = 0;
    print_command(argv, mode);
    pid = fork();
    if (pid < 0) {
        return 1;
    }
    if (pid == 0) {
        execvp(argv[0], argv);
        _exit(127);
    }
    if (waitpid(pid, &status, 0) < 0) {
        return 1;
    }
    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    }
    return 1;
}

static void print_banner(void) {
    printf("TimeVault %s\n", TIMEVAULT_VERSION);
}

static void print_copyright(void) {
    printf("%s\n", TIMEVAULT_COPYRIGHT);
}

static void track_mount(const char *mount) {
    size_t i;
    if (!mount || !*mount) return;
    for (i = 0; i < tracked_mounts_count; i++) {
        if (strcmp(tracked_mounts[i], mount) == 0) return;
    }
    if (tracked_mounts_count == tracked_mounts_cap) {
        size_t next_cap = tracked_mounts_cap ? tracked_mounts_cap * 2 : 4;
        char **tmp = realloc(tracked_mounts, next_cap * sizeof(char *));
        if (!tmp) return;
        tracked_mounts = tmp;
        tracked_mounts_cap = next_cap;
    }
    tracked_mounts[tracked_mounts_count++] = strdup(mount);
}

static void untrack_mount(const char *mount) {
    size_t i;
    if (!mount || !*mount) return;
    for (i = 0; i < tracked_mounts_count; i++) {
        if (strcmp(tracked_mounts[i], mount) == 0) {
            free(tracked_mounts[i]);
            tracked_mounts[i] = tracked_mounts[tracked_mounts_count - 1];
            tracked_mounts_count--;
            return;
        }
    }
}

static void cleanup_mounts(void) {
    size_t i;
    for (i = 0; i < tracked_mounts_count; i++) {
        umount(tracked_mounts[i]);
        free(tracked_mounts[i]);
    }
    free(tracked_mounts);
    tracked_mounts = NULL;
    tracked_mounts_count = 0;
    tracked_mounts_cap = 0;
}

static void handle_signal(int signum) {
    (void)signum;
    cleanup_mounts();
    _exit(1);
}

static int run_nice_ionice(char *const args[], size_t args_count, struct RunMode mode) {
    size_t i;
    char *argv[128];
    size_t idx = 0;

    argv[idx++] = (char *)"nice";
    argv[idx++] = (char *)"-n";
    argv[idx++] = (char *)"19";
    argv[idx++] = (char *)"ionice";
    argv[idx++] = (char *)"-c";
    argv[idx++] = (char *)"3";
    argv[idx++] = (char *)"-n7";

    for (i = 0; i < args_count && idx < 127; i++) {
        argv[idx++] = args[i];
    }
    argv[idx] = NULL;

    if (mode.dry_run) {
        print_command(argv, mode);
        return 0;
    }
    return run_command(argv, mode);
}

static int lock_file(void) {
    for (int attempt = 0; attempt < 3; attempt++) {
        int fd = open(LOCK_FILE, O_CREAT | O_EXCL | O_WRONLY, 0644);
        if (fd >= 0) {
            char buf[32];
            int len = snprintf(buf, sizeof(buf), "%d\n", (int)getpid());
            if (len <= 0 || write(fd, buf, (size_t)len) != len) {
                close(fd);
                unlink(LOCK_FILE);
                return -1;
            }
            close(fd);
            return 1;
        }
        if (errno != EEXIST) {
            return -1;
        }

        FILE *f = fopen(LOCK_FILE, "r");
        if (!f) {
            if (errno == ENOENT) {
                continue;
            }
            return -1;
        }

        char buf[64];
        pid_t pid = 0;
        if (fgets(buf, sizeof(buf), f)) {
            pid = (pid_t)atoi(buf);
        }
        fclose(f);

        if (pid > 0) {
            char proc_path[128];
            snprintf(proc_path, sizeof(proc_path), "/proc/%d", pid);
            if (access(proc_path, F_OK) == 0) {
                return 0;
            }
        }

        if (unlink(LOCK_FILE) != 0) {
            if (errno == ENOENT) {
                continue;
            }
            return -1;
        }
    }
    return 0;
}

static void unlock_file(void) {
    FILE *f;
    char buf[64];
    pid_t pid;

    f = fopen(LOCK_FILE, "r");
    if (!f) return;
    if (fgets(buf, sizeof(buf), f)) {
        pid = (pid_t)atoi(buf);
        if (pid > 0) {
            char proc_path[128];
            snprintf(proc_path, sizeof(proc_path), "/proc/%d", pid);
            if (pid == getpid() && access(proc_path, F_OK) == 0) {
                unlink(LOCK_FILE);
            }
        }
    }
    fclose(f);
}

static char *strdup_safe(const char *s) {
    if (!s) return NULL;
    return strdup(s);
}

static RunPolicy parse_run_policy(const char *value, int *ok) {
    if (!value || !*value) {
        *ok = 1;
        return RUN_AUTO;
    }
    if (strcasecmp(value, "auto") == 0) {
        *ok = 1;
        return RUN_AUTO;
    }
    if (strcasecmp(value, "demand") == 0) {
        *ok = 1;
        return RUN_DEMAND;
    }
    if (strcasecmp(value, "off") == 0) {
        *ok = 1;
        return RUN_OFF;
    }
    *ok = 0;
    return RUN_OFF;
}

static yaml_node_t *mapping_get(yaml_document_t *doc, yaml_node_t *mapping, const char *key) {
    yaml_node_pair_t *pair;
    for (pair = mapping->data.mapping.pairs.start; pair < mapping->data.mapping.pairs.top; pair++) {
        yaml_node_t *k = yaml_document_get_node(doc, pair->key);
        if (k && k->type == YAML_SCALAR_NODE && strcmp((char *)k->data.scalar.value, key) == 0) {
            return yaml_document_get_node(doc, pair->value);
        }
    }
    return NULL;
}

static int add_string(char ***arr, size_t *count, const char *value) {
    char **tmp = realloc(*arr, (*count + 1) * sizeof(char *));
    if (!tmp) return 0;
    *arr = tmp;
    (*arr)[*count] = strdup_safe(value);
    if (!(*arr)[*count]) return 0;
    (*count)++;
    return 1;
}

static int path_has_parent_dir(const char *path) {
    const char *p = path;
    while (*p) {
        while (*p == '/') p++;
        if (!*p) break;
        const char *start = p;
        while (*p && *p != '/') p++;
        size_t len = (size_t)(p - start);
        if (len == 2 && start[0] == '.' && start[1] == '.') {
            return 1;
        }
    }
    return 0;
}

static int path_starts_with(const char *path, const char *prefix) {
    size_t prefix_len = strlen(prefix);
    if (prefix_len == 0) return 0;
    while (prefix_len > 1 && prefix[prefix_len - 1] == '/') {
        prefix_len--;
    }
    if (prefix_len == 1 && prefix[0] == '/') {
        return path[0] == '/';
    }
    if (strncmp(path, prefix, prefix_len) != 0) return 0;
    return path[prefix_len] == '\0' || path[prefix_len] == '/';
}

static int validate_job_paths_config(const struct Job *job, const char *mount_prefix, char *err, size_t err_len) {
    if (!job->dest || !*job->dest) {
        snprintf(err, err_len, "destination path is empty");
        return 0;
    }
    if (!job->mount || !*job->mount) {
        snprintf(err, err_len, "mount is required for all jobs");
        return 0;
    }
    if (job->dest[0] != '/') {
        snprintf(err, err_len, "destination path must be absolute");
        return 0;
    }
    if (job->mount[0] != '/') {
        snprintf(err, err_len, "mount path must be absolute");
        return 0;
    }
    if (path_has_parent_dir(job->dest)) {
        snprintf(err, err_len, "destination path must not contain ..");
        return 0;
    }
    if (path_has_parent_dir(job->mount)) {
        snprintf(err, err_len, "mount path must not contain ..");
        return 0;
    }
    if (mount_prefix && *mount_prefix && !path_starts_with(job->mount, mount_prefix)) {
        snprintf(err, err_len, "mount %s does not start with required prefix %s", job->mount, mount_prefix);
        return 0;
    }
    size_t mount_len = strlen(job->mount);
    while (mount_len > 1 && job->mount[mount_len - 1] == '/') {
        mount_len--;
    }
    size_t dest_len = strlen(job->dest);
    while (dest_len > 1 && job->dest[dest_len - 1] == '/') {
        dest_len--;
    }
    if (dest_len < mount_len ||
        strncmp(job->dest, job->mount, mount_len) != 0 ||
        (dest_len > mount_len && job->dest[mount_len] != '/')) {
        snprintf(err, err_len, "destination %s is not under mount %s", job->dest, job->mount);
        return 0;
    }
    if (dest_len == mount_len && strncmp(job->dest, job->mount, mount_len) == 0) {
        snprintf(err, err_len, "destination must be a subdirectory of mount");
        return 0;
    }
    return 1;
}

static int parse_config(const char *path, struct Config *cfg, char *err, size_t err_len) {
    FILE *f = NULL;
    yaml_parser_t parser;
    yaml_document_t doc;
    yaml_node_t *root = NULL;

    memset(cfg, 0, sizeof(*cfg));

    f = fopen(path, "r");
    if (!f) {
        snprintf(err, err_len, "failed to open %s: %s", path, strerror(errno));
        return 0;
    }
    if (!yaml_parser_initialize(&parser)) {
        snprintf(err, err_len, "failed to initialize yaml parser");
        fclose(f);
        return 0;
    }
    yaml_parser_set_input_file(&parser, f);
    if (!yaml_parser_load(&parser, &doc)) {
        snprintf(err, err_len, "failed to parse yaml");
        yaml_parser_delete(&parser);
        fclose(f);
        return 0;
    }

    root = yaml_document_get_root_node(&doc);
    if (!root || root->type != YAML_MAPPING_NODE) {
        snprintf(err, err_len, "invalid yaml root");
        yaml_document_delete(&doc);
        yaml_parser_delete(&parser);
        fclose(f);
        return 0;
    }

    yaml_node_t *mount_prefix = mapping_get(&doc, root, "mount_prefix");
    if (mount_prefix && mount_prefix->type == YAML_SCALAR_NODE) {
        cfg->mount_prefix = strdup_safe((char *)mount_prefix->data.scalar.value);
    }

    yaml_node_t *excludes = mapping_get(&doc, root, "excludes");
    if (excludes && excludes->type == YAML_SEQUENCE_NODE) {
        yaml_node_item_t *item;
        for (item = excludes->data.sequence.items.start; item < excludes->data.sequence.items.top; item++) {
            yaml_node_t *node = yaml_document_get_node(&doc, *item);
            if (node && node->type == YAML_SCALAR_NODE) {
                if (!add_string(&cfg->excludes, &cfg->excludes_count, (char *)node->data.scalar.value)) {
                    snprintf(err, err_len, "out of memory");
                    yaml_document_delete(&doc);
                    yaml_parser_delete(&parser);
                    fclose(f);
                    return 0;
                }
            }
        }
    }

    yaml_node_t *jobs = mapping_get(&doc, root, "jobs");
    if (!jobs || jobs->type != YAML_SEQUENCE_NODE) {
        snprintf(err, err_len, "missing jobs");
        yaml_document_delete(&doc);
        yaml_parser_delete(&parser);
        fclose(f);
        return 0;
    }

    yaml_node_item_t *item;
    for (item = jobs->data.sequence.items.start; item < jobs->data.sequence.items.top; item++) {
        yaml_node_t *job_node = yaml_document_get_node(&doc, *item);
        if (!job_node || job_node->type != YAML_MAPPING_NODE) {
            continue;
        }
        struct Job job;
        memset(&job, 0, sizeof(job));
        job.copies = 0;
        job.run_policy = RUN_AUTO;

        yaml_node_t *name = mapping_get(&doc, job_node, "name");
        yaml_node_t *source = mapping_get(&doc, job_node, "source");
        yaml_node_t *dest = mapping_get(&doc, job_node, "dest");
        yaml_node_t *copies = mapping_get(&doc, job_node, "copies");
        yaml_node_t *mount = mapping_get(&doc, job_node, "mount");
        yaml_node_t *run = mapping_get(&doc, job_node, "run");
        yaml_node_t *job_excludes = mapping_get(&doc, job_node, "excludes");
        yaml_node_t *job_depends = mapping_get(&doc, job_node, "depends_on");

        if (name && name->type == YAML_SCALAR_NODE) job.name = strdup_safe((char *)name->data.scalar.value);
        if (source && source->type == YAML_SCALAR_NODE) job.source = strdup_safe((char *)source->data.scalar.value);
        if (dest && dest->type == YAML_SCALAR_NODE) job.dest = strdup_safe((char *)dest->data.scalar.value);
        if (mount && mount->type == YAML_SCALAR_NODE) job.mount = strdup_safe((char *)mount->data.scalar.value);
        if (copies && copies->type == YAML_SCALAR_NODE) job.copies = atoi((char *)copies->data.scalar.value);
        if (run && run->type == YAML_SCALAR_NODE) {
            int ok = 0;
            job.run_policy = parse_run_policy((char *)run->data.scalar.value, &ok);
            if (!ok) {
                snprintf(err, err_len, "job %s: invalid run policy %s", job.name ? job.name : "<unknown>", (char *)run->data.scalar.value);
                free_job(&job);
                yaml_document_delete(&doc);
                yaml_parser_delete(&parser);
                fclose(f);
                return 0;
            }
        }

        size_t i;
        for (i = 0; i < cfg->excludes_count; i++) {
            add_string(&job.excludes, &job.excludes_count, cfg->excludes[i]);
        }
        if (job_excludes && job_excludes->type == YAML_SEQUENCE_NODE) {
            yaml_node_item_t *ei;
            for (ei = job_excludes->data.sequence.items.start; ei < job_excludes->data.sequence.items.top; ei++) {
                yaml_node_t *en = yaml_document_get_node(&doc, *ei);
                if (en && en->type == YAML_SCALAR_NODE) {
                    add_string(&job.excludes, &job.excludes_count, (char *)en->data.scalar.value);
                }
            }
        }
        if (job_depends && job_depends->type == YAML_SEQUENCE_NODE) {
            yaml_node_item_t *di;
            for (di = job_depends->data.sequence.items.start; di < job_depends->data.sequence.items.top; di++) {
                yaml_node_t *dn = yaml_document_get_node(&doc, *di);
                if (dn && dn->type == YAML_SCALAR_NODE) {
                    add_string(&job.depends_on, &job.depends_on_count, (char *)dn->data.scalar.value);
                }
            }
        }

        if (!validate_job_paths_config(&job, cfg->mount_prefix, err, err_len)) {
            char prev[256];
            snprintf(prev, sizeof(prev), "%s", err);
            snprintf(err, err_len, "job %s: %s", job.name ? job.name : "<unknown>", prev);
            free_job(&job);
            yaml_document_delete(&doc);
            yaml_parser_delete(&parser);
            fclose(f);
            return 0;
        }

        struct Job *tmp = realloc(cfg->jobs, (cfg->jobs_count + 1) * sizeof(struct Job));
        if (!tmp) {
            snprintf(err, err_len, "out of memory");
            free_job(&job);
            yaml_document_delete(&doc);
            yaml_parser_delete(&parser);
            fclose(f);
            return 0;
        }
        cfg->jobs = tmp;
        cfg->jobs[cfg->jobs_count++] = job;
    }

    yaml_document_delete(&doc);
    yaml_parser_delete(&parser);
    fclose(f);
    return 1;
}

static int mount_in_fstab(const char *mount) {
    FILE *f = fopen("/etc/fstab", "r");
    char line[1024];
    if (!f) return 0;
    while (fgets(line, sizeof(line), f)) {
        char *p = line;
        char *fields[6];
        int n = 0;
        if (*p == '#' || *p == '\n' || *p == '\0') continue;
        while (*p && n < 6) {
            while (*p == ' ' || *p == '\t') p++;
            if (!*p || *p == '\n') break;
            fields[n++] = p;
            while (*p && *p != ' ' && *p != '\t' && *p != '\n') p++;
            if (*p) { *p = '\0'; p++; }
        }
        if (n >= 2 && strcmp(fields[1], mount) == 0) {
            fclose(f);
            return 1;
        }
    }
    fclose(f);
    return 0;
}

static int mount_is_mounted(const char *mount) {
    FILE *f = fopen("/proc/mounts", "r");
    char line[1024];
    if (!f) return 0;
    while (fgets(line, sizeof(line), f)) {
        char *p = line;
        char *fields[6];
        int n = 0;
        while (*p && n < 6) {
            while (*p == ' ' || *p == '\t') p++;
            if (!*p || *p == '\n') break;
            fields[n++] = p;
            while (*p && *p != ' ' && *p != '\t' && *p != '\n') p++;
            if (*p) { *p = '\0'; p++; }
        }
        if (n >= 2 && strcmp(fields[1], mount) == 0) {
            fclose(f);
            return 1;
        }
    }
    fclose(f);
    return 0;
}

static int mount_is_readonly(const char *mount) {
    FILE *f = fopen("/proc/mounts", "r");
    char line[1024];
    if (!f) return -1;
    while (fgets(line, sizeof(line), f)) {
        char *p = line;
        char *fields[6];
        int n = 0;
        while (*p && n < 6) {
            while (*p == ' ' || *p == '\t') p++;
            if (!*p || *p == '\n') break;
            fields[n++] = p;
            while (*p && *p != ' ' && *p != '\t' && *p != '\n') p++;
            if (*p) { *p = '\0'; p++; }
        }
        if (n >= 4 && strcmp(fields[1], mount) == 0) {
            char opts[1024];
            strncpy(opts, fields[3], sizeof(opts) - 1);
            opts[sizeof(opts) - 1] = '\0';
            char *token = strtok(opts, ",");
            while (token) {
                if (strcmp(token, "ro") == 0) {
                    fclose(f);
                    return 1;
                }
                token = strtok(NULL, ",");
            }
            fclose(f);
            return 0;
        }
    }
    fclose(f);
    return -1;
}

static int ensure_unmounted(const char *mount, struct RunMode mode, char *err, size_t err_len) {
    if (!mount_is_mounted(mount)) {
        if (mode.verbose) {
            printf("mount not active, skip umount: %s\n", mount);
        }
        return 1;
    }
    if (mode.verbose) {
        printf("unmounting %s\n", mount);
    }
    char *umount_args[] = { (char *)"umount", (char *)mount, NULL };
    int rc = run_command(umount_args, mode);
    if (rc != 0) {
        snprintf(err, err_len, "umount %s failed with exit code %d", mount, rc);
        return 0;
    }
    if (mount_is_mounted(mount)) {
        snprintf(err, err_len, "umount %s did not detach", mount);
        return 0;
    }
    untrack_mount(mount);
    return 1;
}

static int remove_cb(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf) {
    (void)sb;
    (void)typeflag;
    (void)ftwbuf;
    return remove(fpath);
}

static int remove_symlink_cb(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf) {
    (void)sb;
    (void)ftwbuf;
    if (typeflag == FTW_SL) {
        return remove(fpath);
    }
    return 0;
}

static int remove_dir_recursive(const char *path) {
    return nftw(path, remove_cb, 64, FTW_DEPTH | FTW_PHYS);
}

static int delete_symlinks(const char *path) {
    return nftw(path, remove_symlink_cb, 64, FTW_PHYS);
}

static int compare_strings(const void *a, const void *b) {
    const char *sa = *(const char * const *)a;
    const char *sb = *(const char * const *)b;
    return strcmp(sa, sb);
}

static const char *run_policy_label(RunPolicy policy) {
    switch (policy) {
        case RUN_AUTO:
            return "auto";
        case RUN_DEMAND:
            return "demand";
        case RUN_OFF:
            return "off";
        default:
            return "unknown";
    }
}

static void print_string_list(const char *label, char **items, size_t count) {
    size_t i;
    if (!items || count == 0) {
        printf("  %s: <none>\n", label);
        return;
    }
    printf("  %s: ", label);
    for (i = 0; i < count; i++) {
        if (i > 0) printf(", ");
        printf("%s", items[i]);
    }
    printf("\n");
}

static void print_job_details(const struct Job *job) {
    if (!job) return;
    printf("job: %s\n", job->name ? job->name : "<unnamed>");
    printf("  source: %s\n", job->source ? job->source : "");
    printf("  dest: %s\n", job->dest ? job->dest : "");
    printf("  copies: %d\n", job->copies);
    printf("  mount: %s\n", job->mount ? job->mount : "<unset>");
    printf("  run: %s\n", run_policy_label(job->run_policy));
    print_string_list("depends_on", job->depends_on, job->depends_on_count);
    print_string_list("excludes", job->excludes, job->excludes_count);
}

static int find_job_index(const struct Config *cfg, const char *name) {
    size_t i;
    if (!name || !*name) return -1;
    for (i = 0; i < cfg->jobs_count; i++) {
        if (cfg->jobs[i].name && strcmp(cfg->jobs[i].name, name) == 0) {
            return (int)i;
        }
    }
    return -1;
}

static int job_depends_on(const struct Job *job, const char *name) {
    size_t i;
    if (!job || !name || !*name) return 0;
    for (i = 0; i < job->depends_on_count; i++) {
        if (strcmp(job->depends_on[i], name) == 0) return 1;
    }
    return 0;
}

static int validate_job_names(const struct Config *cfg, char *err, size_t err_len) {
    size_t i;
    for (i = 0; i < cfg->jobs_count; i++) {
        size_t j;
        if (!cfg->jobs[i].name || !*cfg->jobs[i].name) {
            snprintf(err, err_len, "job name is required for dependency ordering");
            return 0;
        }
        for (j = i + 1; j < cfg->jobs_count; j++) {
            if (cfg->jobs[j].name && strcmp(cfg->jobs[i].name, cfg->jobs[j].name) == 0) {
                snprintf(err, err_len, "duplicate job name %s", cfg->jobs[i].name);
                return 0;
            }
        }
    }
    return 1;
}

struct StackItem {
    int idx;
    int parent;
    int has_parent;
};

static int collect_jobs_with_deps(
    const struct Config *cfg,
    char **roots,
    size_t roots_count,
    int *included,
    char *err,
    size_t err_len
) {
    struct StackItem *stack = NULL;
    size_t stack_count = 0;
    size_t stack_cap = 0;
    size_t i;
    for (i = 0; i < roots_count; i++) {
        int idx = find_job_index(cfg, roots[i]);
        if (idx < 0) {
            snprintf(err, err_len, "job not found: %s", roots[i]);
            free(stack);
            return 0;
        }
        if (stack_count == stack_cap) {
            size_t next_cap = stack_cap ? stack_cap * 2 : 8;
            struct StackItem *tmp = realloc(stack, next_cap * sizeof(*stack));
            if (!tmp) {
                snprintf(err, err_len, "out of memory");
                free(stack);
                return 0;
            }
            stack = tmp;
            stack_cap = next_cap;
        }
        stack[stack_count++] = (struct StackItem){ idx, -1, 0 };
    }
    while (stack_count > 0) {
        struct StackItem item = stack[--stack_count];
        struct Job *job = &cfg->jobs[item.idx];
        if (included[item.idx]) {
            continue;
        }
        if (job->run_policy == RUN_OFF) {
            if (item.has_parent && item.parent >= 0) {
                snprintf(err, err_len, "job disabled (off): %s (required by %s)", job->name, cfg->jobs[item.parent].name);
            } else {
                snprintf(err, err_len, "job disabled (off): %s", job->name);
            }
            free(stack);
            return 0;
        }
        included[item.idx] = 1;
        for (i = 0; i < job->depends_on_count; i++) {
            int dep_idx = find_job_index(cfg, job->depends_on[i]);
            if (dep_idx < 0) {
                snprintf(err, err_len, "dependency %s not found for job %s", job->depends_on[i], job->name);
                free(stack);
                return 0;
            }
            if (stack_count == stack_cap) {
                size_t next_cap = stack_cap ? stack_cap * 2 : 8;
                struct StackItem *tmp = realloc(stack, next_cap * sizeof(*stack));
                if (!tmp) {
                    snprintf(err, err_len, "out of memory");
                    free(stack);
                    return 0;
                }
                stack = tmp;
                stack_cap = next_cap;
            }
            stack[stack_count++] = (struct StackItem){ dep_idx, item.idx, 1 };
        }
    }
    free(stack);
    return 1;
}

static int topo_sort_jobs(
    const struct Config *cfg,
    const int *included,
    struct Job **out_jobs,
    size_t *out_count,
    char *err,
    size_t err_len
) {
    size_t i;
    size_t subset_count = 0;
    for (i = 0; i < cfg->jobs_count; i++) {
        if (included[i]) subset_count++;
    }
    if (subset_count == 0) {
        *out_jobs = NULL;
        *out_count = 0;
        return 1;
    }
    int *indegree = calloc(cfg->jobs_count, sizeof(int));
    int *processed = calloc(cfg->jobs_count, sizeof(int));
    struct Job *ordered = malloc(subset_count * sizeof(struct Job));
    if (!indegree || !processed || !ordered) {
        snprintf(err, err_len, "out of memory");
        free(indegree);
        free(processed);
        free(ordered);
        return 0;
    }
    for (i = 0; i < cfg->jobs_count; i++) {
        if (!included[i]) continue;
        size_t d;
        for (d = 0; d < cfg->jobs[i].depends_on_count; d++) {
            int dep_idx = find_job_index(cfg, cfg->jobs[i].depends_on[d]);
            if (dep_idx < 0 || !included[dep_idx]) {
                snprintf(err, err_len, "dependency %s not found for job %s", cfg->jobs[i].depends_on[d], cfg->jobs[i].name);
                free(indegree);
                free(processed);
                free(ordered);
                return 0;
            }
            indegree[i]++;
        }
    }
    size_t output_count = 0;
    while (output_count < subset_count) {
        int found = 0;
        for (i = 0; i < cfg->jobs_count; i++) {
            size_t j;
            if (!included[i] || processed[i]) continue;
            if (indegree[i] != 0) continue;
            ordered[output_count++] = cfg->jobs[i];
            processed[i] = 1;
            found = 1;
            for (j = 0; j < cfg->jobs_count; j++) {
                if (!included[j] || processed[j]) continue;
                if (job_depends_on(&cfg->jobs[j], cfg->jobs[i].name)) {
                    indegree[j]--;
                }
            }
        }
        if (!found) {
            snprintf(err, err_len, "job dependencies contain a cycle");
            free(indegree);
            free(processed);
            free(ordered);
            return 0;
        }
    }
    free(indegree);
    free(processed);
    *out_jobs = ordered;
    *out_count = subset_count;
    return 1;
}

static int expire_old_backups(struct Job *job, const char *dest, struct RunMode mode) {
    DIR *d = opendir(dest);
    struct dirent *e;
    char **backups = NULL;
    size_t backups_count = 0;
    size_t i;

    if (!d) return 0;
    while ((e = readdir(d)) != NULL) {
        if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0 || strcmp(e->d_name, "current") == 0 || strcmp(e->d_name, TIMEVAULT_MARKER) == 0) {
            continue;
        }
        add_string(&backups, &backups_count, e->d_name);
    }
    closedir(d);

    if (backups_count <= (size_t)job->copies) {
        for (i = 0; i < backups_count; i++) free(backups[i]);
        free(backups);
        return 0;
    }

    qsort(backups, backups_count, sizeof(char *), compare_strings);
    size_t to_delete = backups_count - (size_t)job->copies;
    for (i = 0; i < to_delete; i++) {
        char path[PATH_MAX];
        struct stat st;
        snprintf(path, sizeof(path), "%s/%s", dest, backups[i]);
        if (lstat(path, &st) != 0) continue;
        if (S_ISLNK(st.st_mode)) {
            printf("skip symlink delete: %s\n", path);
            continue;
        }
        if (S_ISDIR(st.st_mode)) {
            if (mode.safe_mode || mode.dry_run) {
                if (mode.dry_run) {
                    printf("dry-run: rm -rf %s\n", path);
                } else {
                    printf("skip delete (safe-mode): %s\n", path);
                }
            } else {
                printf("delete: %s\n", path);
                remove_dir_recursive(path);
            }
        } else {
            printf("skip non-dir delete: %s\n", path);
        }
    }

    for (i = 0; i < backups_count; i++) free(backups[i]);
    free(backups);
    return 0;
}

static int create_excludes_file(struct Job *job, const char *path) {
    FILE *f = fopen(path, "w");
    size_t i;
    if (!f) return 0;
    for (i = 0; i < job->excludes_count; i++) {
        fprintf(f, "%s\n", job->excludes[i]);
    }
    fclose(f);
    return 1;
}

static int verify_destination(struct Job *job, const char *mount_prefix, char *err, size_t err_len) {
    char dest_real[PATH_MAX];
    char mount_real[PATH_MAX];

    if (!job->dest || !*job->dest) {
        snprintf(err, err_len, "destination path is empty");
        return 0;
    }
    if (!job->mount || !*job->mount) {
        snprintf(err, err_len, "mount is required for all jobs");
        return 0;
    }
    if (mount_prefix && strncmp(job->mount, mount_prefix, strlen(mount_prefix)) != 0) {
        snprintf(err, err_len, "mount %s does not start with required prefix %s", job->mount, mount_prefix);
        return 0;
    }
    if (!realpath(job->dest, dest_real)) {
        snprintf(err, err_len, "cannot access destination %s: %s", job->dest, strerror(errno));
        return 0;
    }
    if (strcmp(dest_real, "/") == 0) {
        snprintf(err, err_len, "destination resolves to /");
        return 0;
    }
    if (!realpath(job->mount, mount_real)) {
        snprintf(err, err_len, "cannot access mount %s: %s", job->mount, strerror(errno));
        return 0;
    }
    if (strcmp(mount_real, "/") == 0) {
        snprintf(err, err_len, "mount resolves to /");
        return 0;
    }
    size_t mount_len = strlen(mount_real);
    if (strncmp(dest_real, mount_real, mount_len) != 0 ||
        (dest_real[mount_len] != '/' && dest_real[mount_len] != '\0')) {
        snprintf(err, err_len, "destination %s is not under mount %s", dest_real, mount_real);
        return 0;
    }
    if (dest_real[mount_len] == '\0') {
        snprintf(err, err_len, "destination must be a subdirectory of mount");
        return 0;
    }
    if (!mount_is_mounted(mount_real)) {
        snprintf(err, err_len, "mount %s is not mounted", mount_real);
        return 0;
    }
    if (!mount_in_fstab(mount_real)) {
        snprintf(err, err_len, "mount %s not found in /etc/fstab", mount_real);
        return 0;
    }
    char marker[PATH_MAX];
    snprintf(marker, sizeof(marker), "%s/%s", mount_real, TIMEVAULT_MARKER);
    if (access(marker, F_OK) != 0) {
        snprintf(err, err_len, "target device is not a timevault device (missing %s at %s)", TIMEVAULT_MARKER, marker);
        return 0;
    }
    return 1;
}

static int init_timevault(const char *mount, const char *mount_prefix, struct RunMode mode, int force_init, char *err, size_t err_len) {
    char mount_real[PATH_MAX];
    if (!mount || !*mount) {
        snprintf(err, err_len, "mount path is empty");
        return 0;
    }
    if (mount_prefix && strncmp(mount, mount_prefix, strlen(mount_prefix)) != 0) {
        snprintf(err, err_len, "mount %s does not start with required prefix %s", mount, mount_prefix);
        return 0;
    }
    if (!realpath(mount, mount_real)) {
        snprintf(err, err_len, "cannot access mount %s: %s", mount, strerror(errno));
        return 0;
    }
    if (strcmp(mount_real, "/") == 0) {
        snprintf(err, err_len, "mount resolves to /");
        return 0;
    }
    if (!mount_in_fstab(mount_real)) {
        snprintf(err, err_len, "mount %s not found in /etc/fstab", mount_real);
        return 0;
    }
    if (!ensure_unmounted(mount, mode, err, err_len)) {
        return 0;
    }
    char *mount_args[] = { (char *)"mount", (char *)mount, NULL };
    if (run_command(mount_args, mode) != 0) {
        snprintf(err, err_len, "mount %s failed", mount);
        return 0;
    }
    if (!mount_is_mounted(mount_real)) {
        snprintf(err, err_len, "mount %s is not mounted", mount_real);
        return 0;
    }
    track_mount(mount);

    char *remount_rw[] = { (char *)"mount", (char *)"-oremount,rw", (char *)mount, NULL };
    if (run_command(remount_rw, mode) != 0) {
        snprintf(err, err_len, "remount rw %s failed", mount);
        return 0;
    }
    int ro = mount_is_readonly(mount_real);
    if (ro != 0) {
        if (ro < 0) {
            snprintf(err, err_len, "mount %s is not mounted", mount_real);
        } else {
            snprintf(err, err_len, "mount %s is read-only", mount_real);
        }
        char *remount_ro[] = { (char *)"mount", (char *)"-oremount,ro", (char *)mount, NULL };
        run_command(remount_ro, mode);
        char *umount_args[] = { (char *)"umount", (char *)mount, NULL };
        run_command(umount_args, mode);
        untrack_mount(mount);
        return 0;
    }

    DIR *d = opendir(mount_real);
    if (!d) {
        snprintf(err, err_len, "cannot read mount %s: %s", mount_real, strerror(errno));
    } else {
        int empty = 1;
        struct dirent *e;
        while ((e = readdir(d)) != NULL) {
            if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0) {
                continue;
            }
            empty = 0;
            break;
        }
        closedir(d);
        if (!empty && !force_init) {
            snprintf(err, err_len, "mount %s is not empty; aborting init (use --force-init to override)", mount_real);
        }
    }

    if (err[0] == '\0') {
        char marker[PATH_MAX];
        snprintf(marker, sizeof(marker), "%s/%s", mount_real, TIMEVAULT_MARKER);
        if (access(marker, F_OK) == 0) {
            printf("timevault marker already exists: %s\n", marker);
        } else if (mode.dry_run) {
            printf("dry-run: touch %s\n", marker);
        } else {
            FILE *mf = fopen(marker, "w");
            if (!mf) {
                snprintf(err, err_len, "create %s: %s", marker, strerror(errno));
            } else {
                fclose(mf);
            }
        }
    }

    char *remount_ro[] = { (char *)"mount", (char *)"-oremount,ro", (char *)mount, NULL };
    run_command(remount_ro, mode);
    char *umount_args[] = { (char *)"umount", (char *)mount, NULL };
    run_command(umount_args, mode);
    untrack_mount(mount);

    if (err[0] != '\0') {
        return 0;
    }
    return 1;
}

static void format_time(char *buf, size_t len, time_t t) {
    struct tm tm;
    localtime_r(&t, &tm);
    strftime(buf, len, "%d-%m-%Y %H:%M", &tm);
}

static void format_day(char *buf, size_t len, time_t t) {
    struct tm tm;
    localtime_r(&t, &tm);
    strftime(buf, len, "%Y%m%d", &tm);
}

static int backup_jobs(struct Job *jobs, size_t jobs_count, char **rsync_extra, size_t rsync_extra_count, struct RunMode mode, const char *mount_prefix) {
    size_t j;
    for (j = 0; j < jobs_count; j++) {
        struct Job *job = &jobs[j];
        int job_locked = 0;
        if (!mode.dry_run) {
            int lock_rc = lock_file();
            if (lock_rc == 0) {
                printf("timevault is already running\n");
                return 3;
            }
            if (lock_rc < 0) {
                printf("failed to lock %s: %s (need write permission; try sudo or adjust permissions)\n", LOCK_FILE, strerror(errno));
                return 2;
            }
            job_locked = 1;
        }
        if (mode.verbose) {
            const char *policy = job->run_policy == RUN_AUTO ? "auto" : (job->run_policy == RUN_DEMAND ? "demand" : "off");
            printf("job: %s\n", job->name ? job->name : "<unnamed>");
            printf("  run: %s\n", policy);
            printf("  source: %s\n", job->source ? job->source : "");
            printf("  dest: %s\n", job->dest ? job->dest : "");
            printf("  mount: %s\n", job->mount ? job->mount : "<unset>");
            printf("  copies: %d\n", job->copies);
            printf("  excludes: %zu\n", job->excludes_count);
        }

        const char *home = getenv("HOME");
        if (!home) home = "/tmp";
        char tmp_dir[PATH_MAX];
        snprintf(tmp_dir, sizeof(tmp_dir), "%s/tmp", home);
        if (!mode.dry_run) {
            mkdir(tmp_dir, 0755);
        }
        char excludes_path[PATH_MAX];
        snprintf(excludes_path, sizeof(excludes_path), "%s/timevault.excludes", tmp_dir);
        if (mode.dry_run) {
            printf("dry-run: would write excludes file %s\n", excludes_path);
        } else {
            create_excludes_file(job, excludes_path);
        }

        time_t now = time(NULL) - 86400;
        char backup_day[32];
        format_day(backup_day, sizeof(backup_day), now);
        if (mode.verbose) {
            printf("  backup day: %s\n", backup_day);
        }

        if (!job->mount || !*job->mount) {
            printf("skip job %s: mount is required for all jobs\n", job->name ? job->name : "<unnamed>");
            if (job_locked) unlock_file();
            continue;
        }
        char err[256] = {0};
        if (!ensure_unmounted(job->mount, mode, err, sizeof(err))) {
            printf("skip job %s: %s\n", job->name ? job->name : "<unnamed>", err);
            if (job_locked) unlock_file();
            continue;
        }
        char *mount_args[] = { (char *)"mount", job->mount, NULL };
        run_command(mount_args, mode);
        if (mount_is_mounted(job->mount)) {
            track_mount(job->mount);
        }
        char *remount_rw[] = { (char *)"mount", (char *)"-oremount,rw", job->mount, NULL };
        run_command(remount_rw, mode);

        int ro = mount_is_readonly(job->mount);
        if (ro != 0) {
            if (ro < 0) {
                printf("skip job %s: mount %s is not mounted\n", job->name ? job->name : "<unnamed>", job->mount);
            } else {
                printf("skip job %s: mount %s is read-only\n", job->name ? job->name : "<unnamed>", job->mount);
            }
            char *remount_ro[] = { (char *)"mount", (char *)"-oremount,ro", job->mount, NULL };
            run_command(remount_ro, mode);
            char *umount_args[] = { (char *)"umount", job->mount, NULL };
            run_command(umount_args, mode);
            untrack_mount(job->mount);
            if (job_locked) unlock_file();
            continue;
        }

        if (!verify_destination(job, mount_prefix, err, sizeof(err))) {
            printf("skip job %s: %s\n", job->name ? job->name : "<unnamed>", err);
            char *remount_ro[] = { (char *)"mount", (char *)"-oremount,ro", job->mount, NULL };
            run_command(remount_ro, mode);
            char *umount_args[] = { (char *)"umount", job->mount, NULL };
            run_command(umount_args, mode);
            untrack_mount(job->mount);
            if (job_locked) unlock_file();
            continue;
        }

        expire_old_backups(job, job->dest, mode);

        char current_path[PATH_MAX];
        char backup_dir[PATH_MAX];
        snprintf(current_path, sizeof(current_path), "%s/current", job->dest);
        snprintf(backup_dir, sizeof(backup_dir), "%s/%s", job->dest, backup_day);

        struct stat st;
        if (stat(current_path, &st) == 0 && access(backup_dir, F_OK) != 0) {
            if (mode.dry_run) {
                printf("dry-run: mkdir -p %s\n", backup_dir);
            } else {
                mkdir(backup_dir, 0755);
            }
            char cp_src[PATH_MAX];
            snprintf(cp_src, sizeof(cp_src), "%s/.", current_path);
            char *cp_args[] = { (char *)"cp", (char *)"-ralf", cp_src, backup_dir, NULL };
            run_nice_ionice(cp_args, 4, mode);
            if (mode.safe_mode || mode.dry_run) {
                if (mode.dry_run) {
                    printf("dry-run: find %s -type l -delete\n", backup_dir);
                } else {
                    printf("skip symlink cleanup (safe-mode): %s\n", backup_dir);
                }
            } else {
                delete_symlinks(backup_dir);
            }
        }

        char *rsync_args[256];
        size_t idx = 0;
        rsync_args[idx++] = (char *)"rsync";
        rsync_args[idx++] = (char *)"-ar";
        rsync_args[idx++] = (char *)"--stats";
        char exclude_arg[PATH_MAX + 32];
        snprintf(exclude_arg, sizeof(exclude_arg), "--exclude-from=%s", excludes_path);
        rsync_args[idx++] = exclude_arg;
        if (!mode.safe_mode) {
            rsync_args[idx++] = (char *)"--delete-after";
            rsync_args[idx++] = (char *)"--delete-excluded";
        }
        size_t i;
        for (i = 0; i < rsync_extra_count; i++) {
            rsync_args[idx++] = rsync_extra[i];
        }
        rsync_args[idx++] = job->source;
        rsync_args[idx++] = backup_dir;
        rsync_args[idx] = NULL;

        int rc = 1;
        for (i = 0; i < 3; i++) {
            rc = run_nice_ionice(rsync_args, idx, mode);
        }

        if (rc == 0 && access(backup_dir, F_OK) == 0) {
            char current_link[PATH_MAX];
            snprintf(current_link, sizeof(current_link), "%s/current", job->dest);
            struct stat lstat_buf;
            if (lstat(current_link, &lstat_buf) == 0) {
                if (S_ISLNK(lstat_buf.st_mode) || S_ISREG(lstat_buf.st_mode)) {
                    if (mode.safe_mode || mode.dry_run) {
                        if (mode.dry_run) {
                            printf("dry-run: rm -f %s\n", current_link);
                        } else {
                            printf("skip remove (safe-mode): %s\n", current_link);
                        }
                    } else {
                        unlink(current_link);
                    }
                } else if (S_ISDIR(lstat_buf.st_mode)) {
                    printf("skip updating current (directory exists): %s\n", current_link);
                }
            }
            if (access(current_link, F_OK) != 0) {
                if (mode.dry_run) {
                    printf("dry-run: ln -s %s %s\n", backup_day, current_link);
                } else {
                    symlink(backup_day, current_link);
                }
            }
        }

        char *remount_ro[] = { (char *)"mount", (char *)"-oremount,ro", job->mount, NULL };
        run_command(remount_ro, mode);
        char *umount_args[] = { (char *)"umount", job->mount, NULL };
        run_command(umount_args, mode);
        untrack_mount(job->mount);
        if (job_locked) unlock_file();
    }
    return 0;
}

int main(int argc, char **argv) {
    struct RunMode mode = {0, 0, 0};
    char *config_path = (char *)DEFAULT_CONFIG;
    char *init_mount = NULL;
    int force_init = 0;
    char **rsync_extra = NULL;
    size_t rsync_extra_count = 0;
    char **selected_jobs = NULL;
    size_t selected_jobs_count = 0;
    int print_order = 0;
    int show_version = 0;
    int have_lock = 0;
    int rsync_passthrough = 0;

    atexit(cleanup_mounts);
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    int i = 1;
    while (i < argc) {
        if (rsync_passthrough) {
            add_string(&rsync_extra, &rsync_extra_count, argv[i]);
            i++;
            continue;
        }
        if (strcmp(argv[i], "--backup") == 0) {
            i++;
            continue;
        }
        if (strcmp(argv[i], "--dry-run") == 0) {
            mode.dry_run = 1;
            i++;
            continue;
        }
        if (strcmp(argv[i], "--safe") == 0) {
            mode.safe_mode = 1;
            i++;
            continue;
        }
        if (strcmp(argv[i], "--verbose") == 0 || strcmp(argv[i], "-v") == 0) {
            mode.verbose = 1;
            i++;
            continue;
        }
        if (strcmp(argv[i], "--config") == 0) {
            if (i + 1 >= argc) {
                printf("--config requires a path\n");
                return 2;
            }
            config_path = argv[i + 1];
            i += 2;
            continue;
        }
        if (strcmp(argv[i], "--init") == 0) {
            if (i + 1 >= argc) {
                printf("--init requires a mount path\n");
                return 2;
            }
            init_mount = argv[i + 1];
            i += 2;
            continue;
        }
        if (strcmp(argv[i], "--force-init") == 0) {
            if (i + 1 >= argc) {
                printf("--force-init requires a mount path\n");
                return 2;
            }
            if (init_mount) {
                printf("use only one of --init or --force-init\n");
                return 2;
            }
            init_mount = argv[i + 1];
            force_init = 1;
            i += 2;
            continue;
        }
        if (strcmp(argv[i], "--job") == 0) {
            if (i + 1 >= argc) {
                printf("--job requires a name\n");
                return 2;
            }
            add_string(&selected_jobs, &selected_jobs_count, argv[i + 1]);
            i += 2;
            continue;
        }
        if (strcmp(argv[i], "--print-order") == 0) {
            print_order = 1;
            i++;
            continue;
        }
        if (strcmp(argv[i], "--version") == 0) {
            show_version = 1;
            i++;
            continue;
        }
        if (strcmp(argv[i], "--rsync") == 0) {
            rsync_passthrough = 1;
            i++;
            continue;
        }
        if (argv[i][0] == '-') {
            printf("unknown option %s\n", argv[i]);
            return 2;
        }
        add_string(&rsync_extra, &rsync_extra_count, argv[i]);
        i++;
    }

    print_banner();
    if (show_version) {
        print_copyright();
        printf("Project: %s\n", TIMEVAULT_PROJECT_URL);
        printf("License: %s\n", TIMEVAULT_LICENSE);
        return 0;
    }

    char timebuf[64];
    format_time(timebuf, sizeof(timebuf), time(NULL));
    printf("%s\n", timebuf);

    if (init_mount) {
        if (!mode.dry_run && !print_order) {
            int lock_rc = lock_file();
            if (lock_rc == 0) {
                printf("timevault is already running\n");
                return 3;
            }
            if (lock_rc < 0) {
                printf("failed to lock %s: %s (need write permission; try sudo or adjust permissions)\n", LOCK_FILE, strerror(errno));
                return 2;
            }
            have_lock = 1;
        }
        struct Config cfg;
        char err[256] = {0};
        char mount_prefix_buf[PATH_MAX];
        char *mount_prefix = NULL;
        if (access(config_path, F_OK) == 0) {
            if (!parse_config(config_path, &cfg, err, sizeof(err))) {
                printf("failed to load config %s: %s\n", config_path, err);
                if (have_lock) unlock_file();
                return 2;
            }
            if (cfg.mount_prefix) {
                strncpy(mount_prefix_buf, cfg.mount_prefix, sizeof(mount_prefix_buf) - 1);
                mount_prefix_buf[sizeof(mount_prefix_buf) - 1] = '\0';
                mount_prefix = mount_prefix_buf;
            }
            free_config(&cfg);
        }
        if (!init_timevault(init_mount, mount_prefix, mode, force_init, err, sizeof(err))) {
            printf("init failed: %s\n", err);
            if (have_lock) unlock_file();
            return 2;
        }
        printf("initialized timevault at %s\n", init_mount);
        if (have_lock) unlock_file();
        format_time(timebuf, sizeof(timebuf), time(NULL));
        printf("%s\n", timebuf);
        return 0;
    }

    struct Config cfg;
    char err[256] = {0};
    if (!parse_config(config_path, &cfg, err, sizeof(err))) {
        printf("failed to load config %s: %s\n", config_path, err);
        if (have_lock) unlock_file();
        return 2;
    }
    if (!validate_job_names(&cfg, err, sizeof(err))) {
        printf("failed to load config %s: %s\n", config_path, err);
        free_config(&cfg);
        if (have_lock) unlock_file();
        return 2;
    }
    if (cfg.jobs_count > 0) {
        int *all_included = calloc(cfg.jobs_count, sizeof(int));
        struct Job *ordered = NULL;
        size_t ordered_count = 0;
        size_t i;
        if (!all_included) {
            printf("out of memory\n");
            free_config(&cfg);
            if (have_lock) unlock_file();
            return 2;
        }
        for (i = 0; i < cfg.jobs_count; i++) all_included[i] = 1;
        if (!topo_sort_jobs(&cfg, all_included, &ordered, &ordered_count, err, sizeof(err))) {
            printf("failed to load config %s: %s\n", config_path, err);
            free(all_included);
            free(ordered);
            free_config(&cfg);
            if (have_lock) unlock_file();
            return 2;
        }
        free(all_included);
        free(ordered);
    }

    if (mode.verbose) {
        printf("loaded config %s with %zu job(s)\n", config_path, cfg.jobs_count);
        if (cfg.mount_prefix) {
            printf("mount prefix: %s\n", cfg.mount_prefix);
        }
    }

    struct Job *jobs_to_run = NULL;
    size_t jobs_to_run_count = 0;
    char **roots = NULL;
    size_t roots_count = 0;
    if (selected_jobs_count == 0) {
        size_t k;
        for (k = 0; k < cfg.jobs_count; k++) {
            if (cfg.jobs[k].run_policy == RUN_AUTO) {
                char **tmp = realloc(roots, (roots_count + 1) * sizeof(char *));
                if (!tmp) {
                    printf("out of memory\n");
                    free_config(&cfg);
                    if (have_lock) unlock_file();
                    return 2;
                }
                roots = tmp;
                roots[roots_count++] = cfg.jobs[k].name;
            }
        }
    } else {
        roots = selected_jobs;
        roots_count = selected_jobs_count;
    }
    int *included = calloc(cfg.jobs_count, sizeof(int));
    if (!included) {
        printf("out of memory\n");
        free(roots == selected_jobs ? NULL : roots);
        free_config(&cfg);
        if (have_lock) unlock_file();
        return 2;
    }
    err[0] = '\0';
    if (!collect_jobs_with_deps(&cfg, roots, roots_count, included, err, sizeof(err))) {
        if (strncmp(err, "job not found:", 14) == 0) {
            printf("%s\n", err);
            printf("no such job(s) found; aborting\n");
        } else if (strncmp(err, "job disabled (off):", 19) == 0) {
            printf("%s\n", err);
            printf("requested job(s) are disabled; aborting\n");
        } else {
            printf("dependency order failed: %s\n", err);
        }
        free(included);
        if (roots != selected_jobs) free(roots);
        free_config(&cfg);
        if (have_lock) unlock_file();
        return 2;
    }
    if (!topo_sort_jobs(&cfg, included, &jobs_to_run, &jobs_to_run_count, err, sizeof(err))) {
        printf("dependency order failed: %s\n", err);
        free(included);
        if (roots != selected_jobs) free(roots);
        free_config(&cfg);
        if (have_lock) unlock_file();
        return 2;
    }
    free(included);
    if (roots != selected_jobs) free(roots);

    if (jobs_to_run_count == 0) {
        if (selected_jobs_count == 0) {
            printf("no jobs matched (no auto jobs enabled); aborting\n");
        } else {
            printf("no jobs matched selection; aborting\n");
        }
        free_config(&cfg);
        if (have_lock) unlock_file();
        return 2;
    }
    if (print_order) {
        size_t k;
        for (k = 0; k < jobs_to_run_count; k++) {
            print_job_details(&jobs_to_run[k]);
        }
        free(jobs_to_run);
        free_config(&cfg);
        if (have_lock) unlock_file();
        return 0;
    }

    int backup_rc = backup_jobs(jobs_to_run, jobs_to_run_count, rsync_extra, rsync_extra_count, mode, cfg.mount_prefix);
    if (backup_rc != 0) {
        free(jobs_to_run);
        free_config(&cfg);
        for (i = 0; i < (int)rsync_extra_count; i++) free(rsync_extra[i]);
        free(rsync_extra);
        for (i = 0; i < (int)selected_jobs_count; i++) free(selected_jobs[i]);
        free(selected_jobs);
        return backup_rc;
    }

    if (have_lock) unlock_file();
    if (!mode.dry_run) {
        char *sync_args[] = { (char *)"sync", NULL };
        run_command(sync_args, mode);
    }
    format_time(timebuf, sizeof(timebuf), time(NULL));
    printf("%s\n", timebuf);

    free(jobs_to_run);
    free_config(&cfg);
    for (i = 0; i < (int)rsync_extra_count; i++) free(rsync_extra[i]);
    free(rsync_extra);
    for (i = 0; i < (int)selected_jobs_count; i++) free(selected_jobs[i]);
    free(selected_jobs);

    return 0;
}
