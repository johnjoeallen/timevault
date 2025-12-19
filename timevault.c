#include <errno.h>
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
#include <time.h>
#include <unistd.h>
#include <dirent.h>
#include <yaml.h>

#define LOCK_FILE "/var/run/gbackup.pid"
#define DEFAULT_CONFIG "/etc/timevault.yaml"
#define TIMEVAULT_MARKER ".timevault"

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
    FILE *f;
    char buf[64];
    pid_t pid;

    f = fopen(LOCK_FILE, "r");
    if (f) {
        if (fgets(buf, sizeof(buf), f)) {
            pid = (pid_t)atoi(buf);
            if (pid > 0) {
                char proc_path[128];
                snprintf(proc_path, sizeof(proc_path), "/proc/%d", pid);
                if (access(proc_path, F_OK) == 0) {
                    fclose(f);
                    return 0;
                }
            }
        }
        fclose(f);
    }

    f = fopen(LOCK_FILE, "w");
    if (!f) {
        return -1;
    }
    fprintf(f, "%d\n", (int)getpid());
    fclose(f);
    return 1;
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
            if (access(proc_path, F_OK) == 0) {
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

    char *mount_args[] = { (char *)"mount", (char *)mount, NULL };
    if (run_command(mount_args, mode) != 0) {
        snprintf(err, err_len, "mount %s failed", mount);
        return 0;
    }
    if (!mount_is_mounted(mount_real)) {
        snprintf(err, err_len, "mount %s is not mounted", mount_real);
        return 0;
    }

    char *remount_rw[] = { (char *)"mount", (char *)"-oremount,rw", (char *)mount, NULL };
    if (run_command(remount_rw, mode) != 0) {
        snprintf(err, err_len, "remount rw %s failed", mount);
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
        snprintf(excludes_path, sizeof(excludes_path), "%s/gbackup.excludes", tmp_dir);
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
            continue;
        }
        char *mount_args[] = { (char *)"mount", job->mount, NULL };
        run_command(mount_args, mode);
        char *remount_rw[] = { (char *)"mount", (char *)"-oremount,rw", job->mount, NULL };
        run_command(remount_rw, mode);

        char err[256] = {0};
        if (!verify_destination(job, mount_prefix, err, sizeof(err))) {
            printf("skip job %s: %s\n", job->name ? job->name : "<unnamed>", err);
            char *remount_ro[] = { (char *)"mount", (char *)"-oremount,ro", job->mount, NULL };
            run_command(remount_ro, mode);
            char *umount_args[] = { (char *)"umount", job->mount, NULL };
            run_command(umount_args, mode);
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

    int i = 1;
    while (i < argc) {
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
        add_string(&rsync_extra, &rsync_extra_count, argv[i]);
        i++;
    }

    int lock_rc = lock_file();
    if (lock_rc == 0) {
        printf("gbackup is already running\n");
        return 3;
    }
    if (lock_rc < 0) {
        printf("failed to lock %s: %s (need write permission; try sudo or adjust permissions)\n", LOCK_FILE, strerror(errno));
        return 2;
    }

    char timebuf[64];
    format_time(timebuf, sizeof(timebuf), time(NULL));
    printf("%s\n", timebuf);

    if (init_mount) {
        struct Config cfg;
        char err[256] = {0};
        char mount_prefix_buf[PATH_MAX];
        char *mount_prefix = NULL;
        if (access(config_path, F_OK) == 0) {
            if (!parse_config(config_path, &cfg, err, sizeof(err))) {
                printf("failed to load config %s: %s\n", config_path, err);
                unlock_file();
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
            unlock_file();
            return 2;
        }
        printf("initialized timevault at %s\n", init_mount);
        unlock_file();
        format_time(timebuf, sizeof(timebuf), time(NULL));
        printf("%s\n", timebuf);
        return 0;
    }

    struct Config cfg;
    char err[256] = {0};
    if (!parse_config(config_path, &cfg, err, sizeof(err))) {
        printf("failed to load config %s: %s\n", config_path, err);
        unlock_file();
        return 2;
    }

    if (mode.verbose) {
        printf("loaded config %s with %zu job(s)\n", config_path, cfg.jobs_count);
        if (cfg.mount_prefix) {
            printf("mount prefix: %s\n", cfg.mount_prefix);
        }
    }

    struct Job *jobs_to_run = NULL;
    size_t jobs_to_run_count = 0;

    if (selected_jobs_count == 0) {
        size_t k;
        for (k = 0; k < cfg.jobs_count; k++) {
            if (cfg.jobs[k].run_policy == RUN_AUTO) {
                struct Job *tmp = realloc(jobs_to_run, (jobs_to_run_count + 1) * sizeof(struct Job));
                if (!tmp) {
                    printf("out of memory\n");
                    free_config(&cfg);
                    unlock_file();
                    return 2;
                }
                jobs_to_run = tmp;
                jobs_to_run[jobs_to_run_count++] = cfg.jobs[k];
            }
        }
    } else {
        size_t k;
        for (k = 0; k < selected_jobs_count; k++) {
            size_t j;
            int found = 0;
            for (j = 0; j < cfg.jobs_count; j++) {
                if (cfg.jobs[j].name && strcmp(cfg.jobs[j].name, selected_jobs[k]) == 0) {
                    found = 1;
                    if (cfg.jobs[j].run_policy == RUN_OFF) {
                        printf("job disabled (off): %s\n", selected_jobs[k]);
                        printf("requested job(s) are disabled; aborting\n");
                        free_config(&cfg);
                        unlock_file();
                        return 2;
                    }
                    struct Job *tmp = realloc(jobs_to_run, (jobs_to_run_count + 1) * sizeof(struct Job));
                    if (!tmp) {
                        printf("out of memory\n");
                        free_config(&cfg);
                        unlock_file();
                        return 2;
                    }
                    jobs_to_run = tmp;
                    jobs_to_run[jobs_to_run_count++] = cfg.jobs[j];
                    break;
                }
            }
            if (!found) {
                printf("job not found: %s\n", selected_jobs[k]);
                printf("no such job(s) found; aborting\n");
                free_config(&cfg);
                unlock_file();
                return 2;
            }
        }
    }

    if (jobs_to_run_count == 0) {
        if (selected_jobs_count == 0) {
            printf("no jobs matched (no auto jobs enabled); aborting\n");
        } else {
            printf("no jobs matched selection; aborting\n");
        }
        free_config(&cfg);
        unlock_file();
        return 2;
    }

    backup_jobs(jobs_to_run, jobs_to_run_count, rsync_extra, rsync_extra_count, mode, cfg.mount_prefix);

    unlock_file();
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
