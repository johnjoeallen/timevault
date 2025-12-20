#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <csignal>
#include <fcntl.h>
#include <dirent.h>
#include <ftw.h>
#include <limits.h>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <yaml-cpp/yaml.h>

static const char *LOCK_FILE = "/var/run/timevault.pid";
static const char *DEFAULT_CONFIG = "/etc/timevault.yaml";
static const char *TIMEVAULT_MARKER = ".timevault";
static const char *TIMEVAULT_VERSION = "0.1.0";
static const char *TIMEVAULT_LICENSE = "GNU GPL v3 or later";
static const char *TIMEVAULT_COPYRIGHT = "Copyright (C) 2025 John Allen (john.joe.alleN@gmail.com)";
static const char *TIMEVAULT_PROJECT_URL = "https://github.com/johnjoeallen/timevault";

static std::vector<std::string> tracked_mounts;

struct RunMode {
    bool dry_run = false;
    bool safe_mode = false;
    bool verbose = false;
};

enum class RunPolicy {
    Auto,
    Demand,
    Off
};

struct Job {
    std::string name;
    std::string source;
    std::string dest;
    int copies = 0;
    std::string mount;
    RunPolicy run_policy = RunPolicy::Auto;
    std::vector<std::string> excludes;
    std::vector<std::string> depends_on;
};

struct Config {
    std::vector<Job> jobs;
    std::vector<std::string> excludes;
    std::string mount_prefix;
};

static void print_command(const std::vector<std::string> &argv, const RunMode &mode) {
    if (!mode.dry_run && !mode.verbose) return;
    for (size_t i = 0; i < argv.size(); i++) {
        if (i == 0) {
            std::printf("%s", argv[i].c_str());
        } else {
            std::printf(" %s", argv[i].c_str());
        }
    }
    std::printf("\n");
}

static int run_command(const std::vector<std::string> &argv, const RunMode &mode) {
    print_command(argv, mode);
    std::vector<char *> args;
    for (const auto &s : argv) {
        args.push_back(const_cast<char *>(s.c_str()));
    }
    args.push_back(nullptr);

    pid_t pid = fork();
    if (pid < 0) return 1;
    if (pid == 0) {
        execvp(args[0], args.data());
        _exit(127);
    }
    int status = 0;
    if (waitpid(pid, &status, 0) < 0) return 1;
    if (WIFEXITED(status)) return WEXITSTATUS(status);
    return 1;
}

static void print_banner() {
    std::printf("TimeVault %s\n", TIMEVAULT_VERSION);
}

static void print_copyright() {
    std::printf("%s\n", TIMEVAULT_COPYRIGHT);
}

static const char *run_policy_label(RunPolicy policy) {
    switch (policy) {
        case RunPolicy::Auto:
            return "auto";
        case RunPolicy::Demand:
            return "demand";
        case RunPolicy::Off:
            return "off";
        default:
            return "unknown";
    }
}

static void print_string_list(const char *label, const std::vector<std::string> &items) {
    if (items.empty()) {
        std::printf("  %s: <none>\n", label);
        return;
    }
    std::printf("  %s: ", label);
    for (size_t i = 0; i < items.size(); i++) {
        if (i > 0) std::printf(", ");
        std::printf("%s", items[i].c_str());
    }
    std::printf("\n");
}

static void print_job_details(const Job &job) {
    std::printf("job: %s\n", job.name.empty() ? "<unnamed>" : job.name.c_str());
    std::printf("  source: %s\n", job.source.c_str());
    std::printf("  dest: %s\n", job.dest.c_str());
    std::printf("  copies: %d\n", job.copies);
    std::printf("  mount: %s\n", job.mount.empty() ? "<unset>" : job.mount.c_str());
    std::printf("  run: %s\n", run_policy_label(job.run_policy));
    print_string_list("depends_on", job.depends_on);
    print_string_list("excludes", job.excludes);
}

static void track_mount(const std::string &mount) {
    if (mount.empty()) return;
    if (std::find(tracked_mounts.begin(), tracked_mounts.end(), mount) != tracked_mounts.end()) {
        return;
    }
    tracked_mounts.push_back(mount);
}

static void untrack_mount(const std::string &mount) {
    auto it = std::find(tracked_mounts.begin(), tracked_mounts.end(), mount);
    if (it != tracked_mounts.end()) {
        tracked_mounts.erase(it);
    }
}

static void cleanup_mounts() {
    for (const auto &mount : tracked_mounts) {
        umount(mount.c_str());
    }
    tracked_mounts.clear();
}

static void handle_signal(int signum) {
    (void)signum;
    cleanup_mounts();
    _exit(1);
}

static int run_nice_ionice(const std::vector<std::string> &args, const RunMode &mode) {
    std::vector<std::string> argv = {"nice", "-n", "19", "ionice", "-c", "3", "-n7"};
    argv.insert(argv.end(), args.begin(), args.end());
    if (mode.dry_run) {
        print_command(argv, mode);
        return 0;
    }
    return run_command(argv, mode);
}

static int lock_file() {
    for (int attempt = 0; attempt < 3; attempt++) {
        int fd = ::open(LOCK_FILE, O_CREAT | O_EXCL | O_WRONLY, 0644);
        if (fd >= 0) {
            char buf[32];
            int len = std::snprintf(buf, sizeof(buf), "%d\n", static_cast<int>(getpid()));
            if (len <= 0 || ::write(fd, buf, static_cast<size_t>(len)) != len) {
                ::close(fd);
                ::unlink(LOCK_FILE);
                return -1;
            }
            ::close(fd);
            return 1;
        }
        if (errno != EEXIST) return -1;

        FILE *f = std::fopen(LOCK_FILE, "r");
        if (!f) {
            if (errno == ENOENT) {
                continue;
            }
            return -1;
        }

        char buf[64] = {0};
        pid_t pid = 0;
        if (std::fgets(buf, sizeof(buf), f)) {
            pid = static_cast<pid_t>(std::atoi(buf));
        }
        std::fclose(f);

        if (pid > 0) {
            char proc_path[128];
            std::snprintf(proc_path, sizeof(proc_path), "/proc/%d", pid);
            if (access(proc_path, F_OK) == 0) {
                return 0;
            }
        }

        if (::unlink(LOCK_FILE) != 0) {
            if (errno == ENOENT) {
                continue;
            }
            return -1;
        }
    }
    return 0;
}

static void unlock_file() {
    FILE *f = std::fopen(LOCK_FILE, "r");
    if (!f) return;
    char buf[64] = {0};
    if (std::fgets(buf, sizeof(buf), f)) {
        pid_t pid = static_cast<pid_t>(std::atoi(buf));
        if (pid > 0) {
            char proc_path[128];
            std::snprintf(proc_path, sizeof(proc_path), "/proc/%d", pid);
            if (pid == getpid() && access(proc_path, F_OK) == 0) {
                unlink(LOCK_FILE);
            }
        }
    }
    std::fclose(f);
}

static RunPolicy parse_run_policy(const std::string &value, bool *ok) {
    std::string v = value;
    for (auto &c : v) c = static_cast<char>(std::tolower(c));
    if (v.empty() || v == "auto") {
        *ok = true;
        return RunPolicy::Auto;
    }
    if (v == "demand") {
        *ok = true;
        return RunPolicy::Demand;
    }
    if (v == "off") {
        *ok = true;
        return RunPolicy::Off;
    }
    *ok = false;
    return RunPolicy::Off;
}

static bool path_has_parent_dir(const std::string &path) {
    size_t i = 0;
    while (i < path.size()) {
        while (i < path.size() && path[i] == '/') i++;
        if (i >= path.size()) break;
        size_t start = i;
        while (i < path.size() && path[i] != '/') i++;
        size_t len = i - start;
        if (len == 2 && path[start] == '.' && path[start + 1] == '.') {
            return true;
        }
    }
    return false;
}

static bool path_starts_with(const std::string &path, const std::string &prefix) {
    if (prefix.empty()) return false;
    size_t prefix_len = prefix.size();
    while (prefix_len > 1 && prefix[prefix_len - 1] == '/') {
        prefix_len--;
    }
    if (prefix_len == 1 && prefix[0] == '/') {
        return !path.empty() && path[0] == '/';
    }
    if (path.size() < prefix_len) return false;
    if (path.compare(0, prefix_len, prefix, 0, prefix_len) != 0) return false;
    return path.size() == prefix_len || path[prefix_len] == '/';
}

static bool validate_job_paths_config(const Job &job, const std::string &mount_prefix, std::string *err) {
    if (job.dest.empty()) {
        *err = "destination path is empty";
        return false;
    }
    if (job.mount.empty()) {
        *err = "mount is required for all jobs";
        return false;
    }
    if (job.dest[0] != '/') {
        *err = "destination path must be absolute";
        return false;
    }
    if (job.mount[0] != '/') {
        *err = "mount path must be absolute";
        return false;
    }
    if (path_has_parent_dir(job.dest)) {
        *err = "destination path must not contain ..";
        return false;
    }
    if (path_has_parent_dir(job.mount)) {
        *err = "mount path must not contain ..";
        return false;
    }
    if (!mount_prefix.empty() && !path_starts_with(job.mount, mount_prefix)) {
        *err = "mount " + job.mount + " does not start with required prefix " + mount_prefix;
        return false;
    }
    size_t mount_len = job.mount.size();
    while (mount_len > 1 && job.mount[mount_len - 1] == '/') {
        mount_len--;
    }
    size_t dest_len = job.dest.size();
    while (dest_len > 1 && job.dest[dest_len - 1] == '/') {
        dest_len--;
    }
    if (job.dest.size() < mount_len ||
        job.dest.compare(0, mount_len, job.mount, 0, mount_len) != 0 ||
        (job.dest.size() > mount_len && job.dest[mount_len] != '/')) {
        *err = "destination " + job.dest + " is not under mount " + job.mount;
        return false;
    }
    if (dest_len == mount_len && job.dest.compare(0, mount_len, job.mount, 0, mount_len) == 0) {
        *err = "destination must be a subdirectory of mount";
        return false;
    }
    return true;
}

static bool parse_config(const std::string &path, Config *cfg, std::string *err) {
    try {
        YAML::Node root = YAML::LoadFile(path);
        if (root["mount_prefix"]) {
            cfg->mount_prefix = root["mount_prefix"].as<std::string>();
        }
        if (root["excludes"]) {
            for (const auto &ex : root["excludes"]) {
                cfg->excludes.push_back(ex.as<std::string>());
            }
        }
        if (!root["jobs"] || !root["jobs"].IsSequence()) {
            *err = "missing jobs";
            return false;
        }
        for (const auto &node : root["jobs"]) {
            Job job;
            job.name = node["name"].as<std::string>("");
            job.source = node["source"].as<std::string>("");
            job.dest = node["dest"].as<std::string>("");
            job.copies = node["copies"].as<int>(0);
            job.mount = node["mount"].as<std::string>("");
            std::string run = node["run"].as<std::string>("auto");
            bool ok = false;
            job.run_policy = parse_run_policy(run, &ok);
            if (!ok) {
                *err = "job " + job.name + ": invalid run policy " + run;
                return false;
            }
            job.excludes = cfg->excludes;
            if (node["excludes"]) {
                for (const auto &ex : node["excludes"]) {
                    job.excludes.push_back(ex.as<std::string>());
                }
            }
            if (node["depends_on"]) {
                for (const auto &dep : node["depends_on"]) {
                    job.depends_on.push_back(dep.as<std::string>());
                }
            }
            if (!validate_job_paths_config(job, cfg->mount_prefix, err)) {
                *err = "job " + job.name + ": " + *err;
                return false;
            }
            cfg->jobs.push_back(job);
        }
        return true;
    } catch (const std::exception &e) {
        *err = e.what();
        return false;
    }
}

static bool mount_in_fstab(const std::string &mount) {
    FILE *f = std::fopen("/etc/fstab", "r");
    if (!f) return false;
    char line[1024];
    while (std::fgets(line, sizeof(line), f)) {
        char *p = line;
        if (*p == '#' || *p == '\n' || *p == '\0') continue;
        char *fields[6];
        int n = 0;
        while (*p && n < 6) {
            while (*p == ' ' || *p == '\t') p++;
            if (!*p || *p == '\n') break;
            fields[n++] = p;
            while (*p && *p != ' ' && *p != '\t' && *p != '\n') p++;
            if (*p) { *p = '\0'; p++; }
        }
        if (n >= 2 && mount == fields[1]) {
            std::fclose(f);
            return true;
        }
    }
    std::fclose(f);
    return false;
}

static bool mount_is_mounted(const std::string &mount) {
    FILE *f = std::fopen("/proc/mounts", "r");
    if (!f) return false;
    char line[1024];
    while (std::fgets(line, sizeof(line), f)) {
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
        if (n >= 2 && mount == fields[1]) {
            std::fclose(f);
            return true;
        }
    }
    std::fclose(f);
    return false;
}

static int mount_is_readonly(const std::string &mount) {
    FILE *f = std::fopen("/proc/mounts", "r");
    char line[1024];
    if (!f) return -1;
    while (std::fgets(line, sizeof(line), f)) {
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
        if (n >= 4 && mount == fields[1]) {
            char opts[1024];
            std::strncpy(opts, fields[3], sizeof(opts) - 1);
            opts[sizeof(opts) - 1] = '\0';
            char *token = std::strtok(opts, ",");
            while (token) {
                if (std::strcmp(token, "ro") == 0) {
                    std::fclose(f);
                    return 1;
                }
                token = std::strtok(nullptr, ",");
            }
            std::fclose(f);
            return 0;
        }
    }
    std::fclose(f);
    return -1;
}

static bool ensure_unmounted(const std::string &mount, const RunMode &mode, std::string *err) {
    if (!mount_is_mounted(mount)) {
        if (mode.verbose) {
            std::printf("mount not active, skip umount: %s\n", mount.c_str());
        }
        return true;
    }
    if (mode.verbose) {
        std::printf("unmounting %s\n", mount.c_str());
    }
    int rc = run_command({"umount", mount}, mode);
    if (rc != 0) {
        if (err) {
            *err = "umount " + mount + " failed with exit code " + std::to_string(rc);
        }
        return false;
    }
    if (mount_is_mounted(mount)) {
        if (err) {
            *err = "umount " + mount + " did not detach";
        }
        return false;
    }
    untrack_mount(mount);
    return true;
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
    if (typeflag == FTW_SL) return remove(fpath);
    return 0;
}

static int remove_dir_recursive(const std::string &path) {
    return nftw(path.c_str(), remove_cb, 64, FTW_DEPTH | FTW_PHYS);
}

static int delete_symlinks(const std::string &path) {
    return nftw(path.c_str(), remove_symlink_cb, 64, FTW_PHYS);
}

static int expire_old_backups(const Job &job, const std::string &dest, const RunMode &mode) {
    DIR *d = opendir(dest.c_str());
    if (!d) return 0;
    std::vector<std::string> backups;
    struct dirent *e;
    while ((e = readdir(d)) != nullptr) {
        if (std::strcmp(e->d_name, ".") == 0 || std::strcmp(e->d_name, "..") == 0 || std::strcmp(e->d_name, "current") == 0 || std::strcmp(e->d_name, TIMEVAULT_MARKER) == 0) {
            continue;
        }
        backups.emplace_back(e->d_name);
    }
    closedir(d);
    if (backups.size() <= static_cast<size_t>(job.copies)) return 0;
    std::sort(backups.begin(), backups.end());
    size_t to_delete = backups.size() - static_cast<size_t>(job.copies);
    for (size_t i = 0; i < to_delete; i++) {
        std::string path = dest + "/" + backups[i];
        struct stat st;
        if (lstat(path.c_str(), &st) != 0) continue;
        if (S_ISLNK(st.st_mode)) {
            std::printf("skip symlink delete: %s\n", path.c_str());
            continue;
        }
        if (S_ISDIR(st.st_mode)) {
            if (mode.safe_mode || mode.dry_run) {
                if (mode.dry_run) {
                    std::printf("dry-run: rm -rf %s\n", path.c_str());
                } else {
                    std::printf("skip delete (safe-mode): %s\n", path.c_str());
                }
            } else {
                std::printf("delete: %s\n", path.c_str());
                remove_dir_recursive(path);
            }
        } else {
            std::printf("skip non-dir delete: %s\n", path.c_str());
        }
    }
    return 0;
}

static bool create_excludes_file(const Job &job, const std::string &path) {
    FILE *f = std::fopen(path.c_str(), "w");
    if (!f) return false;
    for (const auto &ex : job.excludes) {
        std::fprintf(f, "%s\n", ex.c_str());
    }
    std::fclose(f);
    return true;
}

static bool verify_destination(const Job &job, const std::string &mount_prefix, std::string *err) {
    if (job.dest.empty()) {
        *err = "destination path is empty";
        return false;
    }
    if (job.mount.empty()) {
        *err = "mount is required for all jobs";
        return false;
    }
    if (!mount_prefix.empty() && job.mount.rfind(mount_prefix, 0) != 0) {
        *err = "mount " + job.mount + " does not start with required prefix " + mount_prefix;
        return false;
    }
    char dest_real[PATH_MAX];
    char mount_real[PATH_MAX];
    if (!realpath(job.dest.c_str(), dest_real)) {
        *err = "cannot access destination " + job.dest + ": " + std::strerror(errno);
        return false;
    }
    if (std::strcmp(dest_real, "/") == 0) {
        *err = "destination resolves to /";
        return false;
    }
    if (!realpath(job.mount.c_str(), mount_real)) {
        *err = "cannot access mount " + job.mount + ": " + std::strerror(errno);
        return false;
    }
    if (std::strcmp(mount_real, "/") == 0) {
        *err = "mount resolves to /";
        return false;
    }
    size_t mount_len = std::strlen(mount_real);
    if (std::strncmp(dest_real, mount_real, mount_len) != 0 ||
        (dest_real[mount_len] != '/' && dest_real[mount_len] != '\0')) {
        *err = "destination " + std::string(dest_real) + " is not under mount " + std::string(mount_real);
        return false;
    }
    if (dest_real[mount_len] == '\0') {
        *err = "destination must be a subdirectory of mount";
        return false;
    }
    if (!mount_is_mounted(mount_real)) {
        *err = "mount " + std::string(mount_real) + " is not mounted";
        return false;
    }
    if (!mount_in_fstab(mount_real)) {
        *err = "mount " + std::string(mount_real) + " not found in /etc/fstab";
        return false;
    }
    std::string marker = std::string(mount_real) + "/" + TIMEVAULT_MARKER;
    if (access(marker.c_str(), F_OK) != 0) {
        *err = "target device is not a timevault device (missing " + std::string(TIMEVAULT_MARKER) + " at " + marker + ")";
        return false;
    }
    return true;
}

static int find_job_index(const Config &cfg, const std::string &name) {
    for (size_t i = 0; i < cfg.jobs.size(); i++) {
        if (cfg.jobs[i].name == name) return static_cast<int>(i);
    }
    return -1;
}

static bool job_depends_on(const Job &job, const std::string &name) {
    for (const auto &dep : job.depends_on) {
        if (dep == name) return true;
    }
    return false;
}

static bool validate_job_names(const Config &cfg, std::string *err) {
    std::unordered_set<std::string> names;
    for (const auto &job : cfg.jobs) {
        if (job.name.empty()) {
            *err = "job name is required for dependency ordering";
            return false;
        }
        if (!names.insert(job.name).second) {
            *err = "duplicate job name " + job.name;
            return false;
        }
    }
    return true;
}

struct StackItem {
    int idx;
    int parent;
    bool has_parent;
};

static bool collect_jobs_with_deps(
    const Config &cfg,
    const std::vector<std::string> &roots,
    std::vector<int> *included,
    std::string *err
) {
    std::vector<StackItem> stack;
    for (const auto &name : roots) {
        int idx = find_job_index(cfg, name);
        if (idx < 0) {
            *err = "job not found: " + name;
            return false;
        }
        stack.push_back({idx, -1, false});
    }
    while (!stack.empty()) {
        StackItem item = stack.back();
        stack.pop_back();
        if ((*included)[item.idx]) continue;
        const Job &job = cfg.jobs[item.idx];
        if (job.run_policy == RunPolicy::Off) {
            if (item.has_parent && item.parent >= 0) {
                *err = "job disabled (off): " + job.name + " (required by " + cfg.jobs[item.parent].name + ")";
            } else {
                *err = "job disabled (off): " + job.name;
            }
            return false;
        }
        (*included)[item.idx] = 1;
        for (const auto &dep : job.depends_on) {
            int dep_idx = find_job_index(cfg, dep);
            if (dep_idx < 0) {
                *err = "dependency " + dep + " not found for job " + job.name;
                return false;
            }
            stack.push_back({dep_idx, item.idx, true});
        }
    }
    return true;
}

static bool topo_sort_jobs(
    const Config &cfg,
    const std::vector<int> &included,
    std::vector<Job> *out,
    std::string *err
) {
    std::vector<int> indegree(cfg.jobs.size(), 0);
    std::vector<int> processed(cfg.jobs.size(), 0);
    size_t subset_count = 0;
    for (size_t i = 0; i < cfg.jobs.size(); i++) {
        if (!included[i]) continue;
        subset_count++;
        for (const auto &dep : cfg.jobs[i].depends_on) {
            int dep_idx = find_job_index(cfg, dep);
            if (dep_idx < 0 || !included[dep_idx]) {
                *err = "dependency " + dep + " not found for job " + cfg.jobs[i].name;
                return false;
            }
            indegree[i]++;
        }
    }
    out->clear();
    while (out->size() < subset_count) {
        bool found = false;
        for (size_t i = 0; i < cfg.jobs.size(); i++) {
            if (!included[i] || processed[i]) continue;
            if (indegree[i] != 0) continue;
            out->push_back(cfg.jobs[i]);
            processed[i] = 1;
            found = true;
            for (size_t j = 0; j < cfg.jobs.size(); j++) {
                if (!included[j] || processed[j]) continue;
                if (job_depends_on(cfg.jobs[j], cfg.jobs[i].name)) {
                    indegree[j]--;
                }
            }
        }
        if (!found) {
            *err = "job dependencies contain a cycle";
            return false;
        }
    }
    return true;
}

static bool init_timevault(const std::string &mount, const std::string &mount_prefix, const RunMode &mode, bool force_init, std::string *err) {
    if (mount.empty()) {
        *err = "mount path is empty";
        return false;
    }
    if (!mount_prefix.empty() && mount.rfind(mount_prefix, 0) != 0) {
        *err = "mount " + mount + " does not start with required prefix " + mount_prefix;
        return false;
    }
    char mount_real[PATH_MAX];
    if (!realpath(mount.c_str(), mount_real)) {
        *err = "cannot access mount " + mount + ": " + std::strerror(errno);
        return false;
    }
    if (std::strcmp(mount_real, "/") == 0) {
        *err = "mount resolves to /";
        return false;
    }
    if (!mount_in_fstab(mount_real)) {
        *err = "mount " + std::string(mount_real) + " not found in /etc/fstab";
        return false;
    }
    if (!ensure_unmounted(mount, mode, err)) {
        return false;
    }
    if (run_command({"mount", mount}, mode) != 0) {
        *err = "mount " + mount + " failed";
        return false;
    }
    if (!mount_is_mounted(mount_real)) {
        *err = "mount " + std::string(mount_real) + " is not mounted";
        return false;
    }
    track_mount(mount);
    if (run_command({"mount", "-oremount,rw", mount}, mode) != 0) {
        *err = "remount rw " + mount + " failed";
        return false;
    }
    int ro = mount_is_readonly(mount_real);
    if (ro != 0) {
        if (ro < 0) {
            *err = "mount " + std::string(mount_real) + " is not mounted";
        } else {
            *err = "mount " + std::string(mount_real) + " is read-only";
        }
        run_command({"mount", "-oremount,ro", mount}, mode);
        run_command({"umount", mount}, mode);
        untrack_mount(mount);
        return false;
    }

    DIR *d = opendir(mount_real);
    if (!d) {
        *err = "cannot read mount " + std::string(mount_real) + ": " + std::strerror(errno);
    } else {
        bool empty = true;
        struct dirent *e;
        while ((e = readdir(d)) != nullptr) {
            if (std::strcmp(e->d_name, ".") == 0 || std::strcmp(e->d_name, "..") == 0) continue;
            empty = false;
            break;
        }
        closedir(d);
        if (!empty && !force_init) {
            *err = "mount " + std::string(mount_real) + " is not empty; aborting init (use --force-init to override)";
        }
    }

    if (err->empty()) {
        std::string marker = std::string(mount_real) + "/" + TIMEVAULT_MARKER;
        if (access(marker.c_str(), F_OK) == 0) {
            std::printf("timevault marker already exists: %s\n", marker.c_str());
        } else if (mode.dry_run) {
            std::printf("dry-run: touch %s\n", marker.c_str());
        } else {
            FILE *mf = std::fopen(marker.c_str(), "w");
            if (!mf) {
                *err = "create " + marker + ": " + std::strerror(errno);
            } else {
                std::fclose(mf);
            }
        }
    }

    run_command({"mount", "-oremount,ro", mount}, mode);
    run_command({"umount", mount}, mode);
    untrack_mount(mount);

    return err->empty();
}

static void format_time(char *buf, size_t len, time_t t) {
    struct tm tm;
    localtime_r(&t, &tm);
    std::strftime(buf, len, "%d-%m-%Y %H:%M", &tm);
}

static void format_day(char *buf, size_t len, time_t t) {
    struct tm tm;
    localtime_r(&t, &tm);
    std::strftime(buf, len, "%Y%m%d", &tm);
}

static void backup_jobs(const std::vector<Job> &jobs, const std::vector<std::string> &rsync_extra, const RunMode &mode, const std::string &mount_prefix) {
    for (const auto &job : jobs) {
        bool job_locked = false;
        if (!mode.dry_run) {
            int lock_rc = lock_file();
            if (lock_rc == 0) {
                std::printf("timevault is already running\n");
                std::exit(3);
            }
            if (lock_rc < 0) {
                std::printf("failed to lock %s: %s (need write permission; try sudo or adjust permissions)\n", LOCK_FILE, std::strerror(errno));
                std::exit(2);
            }
            job_locked = true;
        }
        if (mode.verbose) {
            const char *policy = job.run_policy == RunPolicy::Auto ? "auto" : (job.run_policy == RunPolicy::Demand ? "demand" : "off");
            std::printf("job: %s\n", job.name.c_str());
            std::printf("  run: %s\n", policy);
            std::printf("  source: %s\n", job.source.c_str());
            std::printf("  dest: %s\n", job.dest.c_str());
            std::printf("  mount: %s\n", job.mount.empty() ? "<unset>" : job.mount.c_str());
            std::printf("  copies: %d\n", job.copies);
            std::printf("  excludes: %zu\n", job.excludes.size());
        }

        const char *home = getenv("HOME");
        if (!home) home = "/tmp";
        char tmp_dir[PATH_MAX];
        std::snprintf(tmp_dir, sizeof(tmp_dir), "%s/tmp", home);
        if (!mode.dry_run) {
            mkdir(tmp_dir, 0755);
        }
        char excludes_path[PATH_MAX];
        std::snprintf(excludes_path, sizeof(excludes_path), "%s/timevault.excludes", tmp_dir);
        if (mode.dry_run) {
            std::printf("dry-run: would write excludes file %s\n", excludes_path);
        } else {
            create_excludes_file(job, excludes_path);
        }

        time_t now = time(nullptr) - 86400;
        char backup_day[32];
        format_day(backup_day, sizeof(backup_day), now);
        if (mode.verbose) {
            std::printf("  backup day: %s\n", backup_day);
        }

        if (job.mount.empty()) {
            std::printf("skip job %s: mount is required for all jobs\n", job.name.c_str());
            if (job_locked) unlock_file();
            continue;
        }
        std::string err;
        if (!ensure_unmounted(job.mount, mode, &err)) {
            std::printf("skip job %s: %s\n", job.name.c_str(), err.c_str());
            if (job_locked) unlock_file();
            continue;
        }
        run_command({"mount", job.mount}, mode);
        if (mount_is_mounted(job.mount)) {
            track_mount(job.mount);
        }
        run_command({"mount", "-oremount,rw", job.mount}, mode);

        int ro = mount_is_readonly(job.mount);
        if (ro != 0) {
            if (ro < 0) {
                std::printf("skip job %s: mount %s is not mounted\n", job.name.c_str(), job.mount.c_str());
            } else {
                std::printf("skip job %s: mount %s is read-only\n", job.name.c_str(), job.mount.c_str());
            }
            run_command({"mount", "-oremount,ro", job.mount}, mode);
            run_command({"umount", job.mount}, mode);
            untrack_mount(job.mount);
            if (job_locked) unlock_file();
            continue;
        }

        if (!verify_destination(job, mount_prefix, &err)) {
            std::printf("skip job %s: %s\n", job.name.c_str(), err.c_str());
            run_command({"mount", "-oremount,ro", job.mount}, mode);
            run_command({"umount", job.mount}, mode);
            untrack_mount(job.mount);
            if (job_locked) unlock_file();
            continue;
        }

        expire_old_backups(job, job.dest, mode);

        std::string current_path = job.dest + "/current";
        std::string backup_dir = job.dest + "/" + backup_day;
        struct stat st;
        if (stat(current_path.c_str(), &st) == 0 && access(backup_dir.c_str(), F_OK) != 0) {
            if (mode.dry_run) {
                std::printf("dry-run: mkdir -p %s\n", backup_dir.c_str());
            } else {
                mkdir(backup_dir.c_str(), 0755);
            }
            std::string cp_src = current_path + "/.";
            run_nice_ionice({"cp", "-ralf", cp_src, backup_dir}, mode);
            if (mode.safe_mode || mode.dry_run) {
                if (mode.dry_run) {
                    std::printf("dry-run: find %s -type l -delete\n", backup_dir.c_str());
                } else {
                    std::printf("skip symlink cleanup (safe-mode): %s\n", backup_dir.c_str());
                }
            } else {
                delete_symlinks(backup_dir);
            }
        }

        std::vector<std::string> rsync_args = {"rsync", "-ar", "--stats", std::string("--exclude-from=") + excludes_path};
        if (!mode.safe_mode) {
            rsync_args.push_back("--delete-after");
            rsync_args.push_back("--delete-excluded");
        }
        for (const auto &arg : rsync_extra) rsync_args.push_back(arg);
        rsync_args.push_back(job.source);
        rsync_args.push_back(backup_dir);

        int rc = 1;
        for (int i = 0; i < 3; i++) {
            rc = run_nice_ionice(rsync_args, mode);
        }

        if (rc == 0 && access(backup_dir.c_str(), F_OK) == 0) {
            std::string current_link = job.dest + "/current";
            struct stat lstat_buf;
            if (lstat(current_link.c_str(), &lstat_buf) == 0) {
                if (S_ISLNK(lstat_buf.st_mode) || S_ISREG(lstat_buf.st_mode)) {
                    if (mode.safe_mode || mode.dry_run) {
                        if (mode.dry_run) {
                            std::printf("dry-run: rm -f %s\n", current_link.c_str());
                        } else {
                            std::printf("skip remove (safe-mode): %s\n", current_link.c_str());
                        }
                    } else {
                        unlink(current_link.c_str());
                    }
                } else if (S_ISDIR(lstat_buf.st_mode)) {
                    std::printf("skip updating current (directory exists): %s\n", current_link.c_str());
                }
            }
            if (access(current_link.c_str(), F_OK) != 0) {
                if (mode.dry_run) {
                    std::printf("dry-run: ln -s %s %s\n", backup_day, current_link.c_str());
                } else {
                    symlink(backup_day, current_link.c_str());
                }
            }
        }

        run_command({"mount", "-oremount,ro", job.mount}, mode);
        run_command({"umount", job.mount}, mode);
        untrack_mount(job.mount);
        if (job_locked) unlock_file();
    }
}

int main(int argc, char **argv) {
    RunMode mode;
    std::string config_path = DEFAULT_CONFIG;
    std::string init_mount;
    bool force_init = false;
    std::vector<std::string> rsync_extra;
    std::vector<std::string> selected_jobs;
    bool print_order = false;
    bool show_version = false;
    bool have_lock = false;
    bool rsync_passthrough = false;

    std::atexit(cleanup_mounts);
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (rsync_passthrough) {
            rsync_extra.push_back(arg);
            continue;
        }
        if (arg == "--backup") {
            continue;
        } else if (arg == "--dry-run") {
            mode.dry_run = true;
        } else if (arg == "--safe") {
            mode.safe_mode = true;
        } else if (arg == "--verbose" || arg == "-v") {
            mode.verbose = true;
        } else if (arg == "--config") {
            if (i + 1 >= argc) {
                std::printf("--config requires a path\n");
                return 2;
            }
            config_path = argv[++i];
        } else if (arg == "--init") {
            if (i + 1 >= argc) {
                std::printf("--init requires a mount path\n");
                return 2;
            }
            init_mount = argv[++i];
        } else if (arg == "--force-init") {
            if (i + 1 >= argc) {
                std::printf("--force-init requires a mount path\n");
                return 2;
            }
            if (!init_mount.empty()) {
                std::printf("use only one of --init or --force-init\n");
                return 2;
            }
            init_mount = argv[++i];
            force_init = true;
        } else if (arg == "--job") {
            if (i + 1 >= argc) {
                std::printf("--job requires a name\n");
                return 2;
            }
            selected_jobs.push_back(argv[++i]);
        } else if (arg == "--print-order") {
            print_order = true;
        } else if (arg == "--version") {
            show_version = true;
        } else if (arg == "--rsync") {
            rsync_passthrough = true;
        } else if (!arg.empty() && arg[0] == '-') {
            std::printf("unknown option %s\n", arg.c_str());
            return 2;
        } else {
            rsync_extra.push_back(arg);
        }
    }

    print_banner();
    if (show_version) {
        print_copyright();
        std::printf("Project: %s\n", TIMEVAULT_PROJECT_URL);
        std::printf("License: %s\n", TIMEVAULT_LICENSE);
        return 0;
    }

    char timebuf[64];
    format_time(timebuf, sizeof(timebuf), std::time(nullptr));
    std::printf("%s\n", timebuf);

    if (!init_mount.empty()) {
        if (!mode.dry_run && !print_order) {
            int lock_rc = lock_file();
            if (lock_rc == 0) {
                std::printf("timevault is already running\n");
                return 3;
            }
            if (lock_rc < 0) {
                std::printf("failed to lock %s: %s (need write permission; try sudo or adjust permissions)\n", LOCK_FILE, std::strerror(errno));
                return 2;
            }
            have_lock = true;
        }
        Config cfg;
        std::string err;
        std::string mount_prefix;
        if (access(config_path.c_str(), F_OK) == 0) {
            if (!parse_config(config_path, &cfg, &err)) {
                std::printf("failed to load config %s: %s\n", config_path.c_str(), err.c_str());
                if (have_lock) unlock_file();
                return 2;
            }
            mount_prefix = cfg.mount_prefix;
        }
        if (!init_timevault(init_mount, mount_prefix, mode, force_init, &err)) {
            std::printf("init failed: %s\n", err.c_str());
            if (have_lock) unlock_file();
            return 2;
        }
        std::printf("initialized timevault at %s\n", init_mount.c_str());
        if (have_lock) unlock_file();
        format_time(timebuf, sizeof(timebuf), std::time(nullptr));
        std::printf("%s\n", timebuf);
        return 0;
    }

    Config cfg;
    std::string err;
    if (!parse_config(config_path, &cfg, &err)) {
        std::printf("failed to load config %s: %s\n", config_path.c_str(), err.c_str());
        if (have_lock) unlock_file();
        return 2;
    }
    if (!validate_job_names(cfg, &err)) {
        std::printf("failed to load config %s: %s\n", config_path.c_str(), err.c_str());
        if (have_lock) unlock_file();
        return 2;
    }
    if (!cfg.jobs.empty()) {
        std::vector<int> all_included(cfg.jobs.size(), 1);
        std::vector<Job> ordered;
        if (!topo_sort_jobs(cfg, all_included, &ordered, &err)) {
            std::printf("failed to load config %s: %s\n", config_path.c_str(), err.c_str());
            if (have_lock) unlock_file();
            return 2;
        }
    }

    std::vector<std::string> roots;
    if (selected_jobs.empty()) {
        for (const auto &job : cfg.jobs) {
            if (job.run_policy == RunPolicy::Auto) {
                roots.push_back(job.name);
            }
        }
    } else {
        std::unordered_set<std::string> seen;
        for (const auto &name : selected_jobs) {
            if (seen.insert(name).second) {
                roots.push_back(name);
            }
        }
    }
    std::vector<int> included(cfg.jobs.size(), 0);
    err.clear();
    if (!collect_jobs_with_deps(cfg, roots, &included, &err)) {
        if (err.rfind("job not found:", 0) == 0) {
            std::printf("%s\n", err.c_str());
            std::printf("no such job(s) found; aborting\n");
        } else if (err.rfind("job disabled (off):", 0) == 0) {
            std::printf("%s\n", err.c_str());
            std::printf("requested job(s) are disabled; aborting\n");
        } else {
            std::printf("dependency order failed: %s\n", err.c_str());
        }
        if (have_lock) unlock_file();
        return 2;
    }
    std::vector<Job> jobs_to_run;
    if (!topo_sort_jobs(cfg, included, &jobs_to_run, &err)) {
        std::printf("dependency order failed: %s\n", err.c_str());
        if (have_lock) unlock_file();
        return 2;
    }

    if (jobs_to_run.empty()) {
        if (selected_jobs.empty()) {
            std::printf("no jobs matched (no auto jobs enabled); aborting\n");
        } else {
            std::printf("no jobs matched selection; aborting\n");
        }
        if (have_lock) unlock_file();
        return 2;
    }
    if (print_order) {
        for (const auto &job : jobs_to_run) {
            print_job_details(job);
        }
        if (have_lock) unlock_file();
        return 0;
    }

    if (mode.verbose) {
        std::printf("loaded config %s with %zu job(s)\n", config_path.c_str(), jobs_to_run.size());
        if (!cfg.mount_prefix.empty()) {
            std::printf("mount prefix: %s\n", cfg.mount_prefix.c_str());
        }
    }

    backup_jobs(jobs_to_run, rsync_extra, mode, cfg.mount_prefix);

    if (have_lock) unlock_file();
    if (!mode.dry_run) {
        run_command({"sync"}, mode);
    }
    format_time(timebuf, sizeof(timebuf), std::time(nullptr));
    std::printf("%s\n", timebuf);
    return 0;
}
