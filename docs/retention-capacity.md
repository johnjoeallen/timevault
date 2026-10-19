# Retention and capacity guide

## Three separate settings

People often treat these as one. They are independent.

| Concept | What it is | Where it is set |
|---|---|---|
| **Backup frequency** | How often a job runs: hourly, daily, weekly, on demand. | Your scheduler (`timevault.timer`, cron, manual runs) and the job's `run:` policy. |
| **Generation retention** | How many generations of one job Timevault keeps **on one disk**. | `copies:` on the job. |
| **Disk rotation interval** | How often a physical disk is connected or disconnected. | You. Timevault needs no setting for it. |

`copies: 20` means "keep 20 generations on each disk", **not** "keep 20 days". How many calendar days 20 generations cover depends on how often the job runs *and* on how long each disk is connected.

### One generation per day

Generations are named by day. If a job runs more than once on the same backup day, the later run builds a fresh generation and replaces the earlier one (the swap is a rename; an interrupted swap is repaired on the next run). So an hourly schedule still produces one retained generation per day, holding the state at the last run of that day. Frequencies finer than a day add freshness, not extra restore points.

### How pruning works

- After a new generation is published, the oldest generations beyond `copies` are deleted. Names sort by date, so oldest means smallest `YYYYMMDD`.
- The generation `current` points to is never deleted.
- Pruning runs only after a successful publish. A failed backup deletes nothing.
- Timevault never deletes completed generations to make room for a backup that is failing. See [Out of space](#out-of-space).

## How many calendar days do my generations cover?

These are illustrative calculations, not guarantees. They assume every scheduled run succeeds and the pattern has been running long enough to be steady.

Let *g* be the number of generations a disk gains per calendar day on average, and *copies* the retention. The disk covers roughly *copies ÷ g* calendar days, with gaps.

| Arrangement | Backups | Generations per disk | `copies` | Approximate span of one disk |
|---|---|---|---|---|
| One disk always connected | daily | 7 per week | 20 | 20 days, no gaps |
| Two disks alternating weekly | daily | 7 per 14 days | 20 | about 40 days (5-6 weeks), with a 7-day gap between each 7-day run |
| Three disks, two connected at a time (Strategy D) | daily | 14 per 21 days | 20 | about 30 days, with a 7-day gap every 3 weeks |
| Three disks, one connected at a time (Strategy E) | daily | 7 per 21 days | 20 | about 60 days, three 7-day blocks per 3 weeks |
| Two disks alternating weekly | weekly | 1 per 2 weeks | 8 | about 16 weeks |

Notes:

- No single disk in a rotating arrangement holds an uninterrupted daily history. Other disks hold the days a given disk missed.
- The more disks share the work, the longer each disk's retained generations stretch across the calendar, but the coarser the history on that disk.
- Per-job `copies` is independent: a job with `copies: 7` and a job with `copies: 60` can share a disk.

### The recovery limitation

A disk cannot contain a file that was created and deleted while the disk was disconnected. If you need a stronger guarantee for recently deleted files, keep two disks connected (Strategy C, D or G) or rotate more often.

## Capacity: how hard-linked generations use space

### Unchanged files

An unchanged file is a hard link to the same data as in the previous generation. It uses no extra data blocks. It still uses a directory entry and some filesystem metadata, so a generation of millions of tiny files is not free.

### Modified files

A changed file is stored again in full for the new generation. The older generation keeps the old copy until that generation is pruned. Large files that change a little every day (databases, VM images, mailboxes) are the biggest consumers.

### Deleted files

A file deleted from the source disappears from newer generations, but its data stays on the disk until every generation that references it has been pruned. A big deletion at the source does not free space on the backup disk right away.

### Moved and renamed files

Matching is by relative path. If you move a large tree or rename many files, rsync sees new paths and stores that content again, even though the old copy still exists in earlier generations. After a large reorganisation, expect the next backup to need space close to the size of the moved data, and that space stays in use until the old generations holding the old paths expire. Detection of moved files is planned but not yet available.

### A disk returning after time away

A disk returning after a week does not need a full backup. It needs room for what changed, was added or was removed at the source since its last generation. A longer absence can mean a larger single increment.

### Headroom

Keep free space for:

- changed and new files since the last generation,
- the in-progress generation (it is built in staging next to the completed ones, so it needs real room until it is published),
- filesystem metadata,
- unexpected source growth,
- large reorganisations of the source.

There is no universal percentage that is safe. Watch `timevault disk df` and act when free space shrinks faster than your pruning frees it.

## Out of space

If the disk fills during a run, rsync fails (typically with exit code 11 and a "No space left on device" message). Timevault then:

- publishes nothing and leaves `current` where it was,
- prunes nothing, and
- records a failed job in the report with the rsync exit code and message, followed by "the backup disk is out of space: no generation was published and no completed generation was deleted".

Completed generations stay intact. Timevault does not delete old generations to make room. The partial staging directory is discarded on the next run when a previous generation exists.

To recover without losing recovery points:

1. Run `timevault disk df` to confirm the disk is full, and `timevault disk du -sh <disk>:/<job>` to see what each job uses.
2. Choose how to create space. Options that keep history intact: use a larger disk, add another disk, exclude large data you can regenerate (`excludes`), or move a job to a different disk with `diskIds`.
3. If you decide to give up history, that is your decision to make explicitly: lower `copies` for the job and remove the oldest generation by hand (Timevault has no command for this). Deleting one old generation frees only the data that no newer generation shares.
4. Run the backup again.

Remember that a run needs room for a new in-progress generation. If the disk is nearly full, a run can fail again even after a small cleanup.
