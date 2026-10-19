import glob, re, subprocess, sys, tempfile, os

bad = 0
for f in sorted(glob.glob('docs/*.md')):
    text = open(f).read()
    for i, m in enumerate(re.finditer(r'```yaml\n(.*?)```', text, re.S)):
        block = m.group(1)
        disk = 'backupDisks:\n' + ''.join('  - diskId: %s\n    fsUuid: 11111111-1111-1111-1111-11111111111%d\n' % (n, k) for k, n in enumerate(['x', 'usb-a', 'usb-b']))
        job = 'jobs:\n  - name: j\n    source: /tmp\n    copies: 1\n    run: auto\n'
        if 'jobs:' not in block and 'backupDisks' not in block:
            continue
        if 'backupDisks' not in block:
            block = disk + block
        if 'jobs:' not in block:
            block = block + job
        with tempfile.NamedTemporaryFile('w', suffix='.yaml', delete=False) as t:
            t.write(block)
        r = subprocess.run([os.environ.get('TIMEVAULT_BIN', 'target/debug/timevault'), '--config', t.name, '--job', 'all', '--print-order'],
                           capture_output=True, text=True)
        os.unlink(t.name)
        status = 'ok' if r.returncode == 0 else 'FAIL'
        if r.returncode != 0:
            bad = 1
        print(f, 'block', i, status, (r.stderr or r.stdout).strip()[:200] if r.returncode else '')
sys.exit(bad)
