#! /usr/bin/php -Cq
<?php
    $lockFile = "/var/run/gbackup.pid";
    $configFile = "/etc/gbackup.xml";
    $rsyncOptions = "";

    /**
     * Create the lock file.
     * This function creates a lockfile with the pid of the current
     * process.
     *
     * @return false if the file exists, and the pid is a current process
     * @return true if the file does not exist, or the pid is not a current process
      */
    function lock()
    {
        global $lockFile;
        $pid = @file_get_contents($lockFile);

        if ($pid && file_exists("/proc/$pid"))
        {
            return false;
        }

        $f = fopen($lockFile, "w");
        fwrite($f, getmypid());
        fclose($f);

        return true;
    }

    /**
     *
      */
    function unlock()
    {
        global $lockFile;
        $pid = @file_get_contents($lockFile);

        if ($pid && file_exists("/proc/$pid"))
        {
            unlink($lockFile);
        }

    }

    /**
     *
      */
    function getConfig()
    {
        global $configFile;
        $config = new StdClass();

        if (file_exists($configFile))
        {
            $doc = new DOMDocument();
            $doc->loadXML(file_get_contents($configFile));
            $jobElements = $doc->getElementsByTagName("job");
            $excludeElements = $doc->getElementsByTagName("exclude");
            $jobs = array();

            foreach($jobElements as $jobElement)
            {
                $job = new StdClass();

                foreach (array("id", "source", "dest", "copies", "mount", "enabled") as $attr)
                {
                    $job->$attr = $jobElement->getAttribute($attr);
                }

                if ($job->enabled)
                {
                    $excludes = array();
                    foreach($excludeElements as $excludeElement)
                    {
                        $exclude = new StdClass();
                        $exclude->path = $excludeElement->getAttribute("path");
                        $id = $excludeElement->getAttribute("id");

                        if ($id == "" || $id == $job->id)
                        {
                            $excludes[] = $exclude;
                        }
                    }

                    $job->excludes = $excludes;
                    $jobs[] = $job;
                }
            }

        }
        else
        {
            $conn = pg_connect("dbname=gbackup user=gbackup host=localhost");
            $jobsResult = pg_query($conn, "select id, source, dest, copies, mount from jobs where enabled=true order by priority");
            $jobs = array();

            while ($job = pg_fetch_object($jobsResult))
            {
                $job->excludes = array();

                $excludesResult = pg_query($conn, "select path from excludes where job_id = " . $job->id . " or job_id is null");

                while ($exclude = pg_fetch_object($excludesResult))
                {
                    $job->excludes[$exclude->path] = $exclude;
                }

                $jobs[$job->id] = $job;
            }

            pg_close($conn);
        }

        $config->jobs = $jobs;

        return $config;
    }

    /**
     *
      */
    function createExcludesFile($job, $filename)
    {
        $f = fopen($filename, "w");

        foreach ($job->excludes as $exclude)
        {
            fputs($f,  $exclude->path . "\n");
        }

        fclose($f);
    }

    /**
     *
      */
    function findPreviousBackup($job)
    {
        if (file_exists($job->dest . "/current"))
        {
            return "current";
        }
        else
        {
            return "";
        }
    }

    /**
     *
      */
    function expireOldBackups($job)
    {
        $backups = array();

        if ($d = opendir($job->dest))
        {
            while ($e = readdir($d))
            {
                if ($e != "." && $e != ".." && $e != "current")
                {
                    $backups[] = $e;
                }
            }

            closedir($d);

            if (count($backups))
            {
                sort($backups);

                if (count($backups) > $job->copies)
                {
                    $todelete = count($backups) - $job->copies;

                    for ($i = 0; $i < $todelete; $i++)
                    {
                        $cmd = "nice -n 19 ionice -c 3 -n7 rm -rf " . $job->dest . "/" . $backups[$i];
                        print "$cmd\n";
                        system($cmd);
                    }
                }
            }
        }
    }

    /**
     *
     */
    function backup($config)
    {
        global $rsyncOptions;
        foreach ($config->jobs as $job)
        {
            //print_r($job);
            @mkdir(getenv("HOME") . "/tmp", 0755, true);
            $excludesFile = getenv("HOME") . "/tmp/gbackup.excludes";

            createExcludesFile($job, $excludesFile);
            $backupDay = date("Ymd", time()-86400);

            if ($job->mount != "")
            {
                // ensure it's mounted first
                $cmd = "mount " . $job->mount;
                print "$cmd\n";
                system($cmd);

                // remount read/write
                $cmd = "mount -oremount,rw " . $job->mount;
                print "$cmd\n";
                system($cmd);
            }

            expireOldBackups($job);

            $cmd = "nice -n 19 ionice -c 3 -n7 rsync $rsyncOptions -ar --delete-after --delete-excluded --stats --exclude-from=$excludesFile " . $job->source . " ";

            // only create the hard links if the dest directory does not already exist
            if (file_exists($job->dest . "/current") && !file_exists($job->dest . "/$backupDay"))
            {
                @mkdir($job->dest . "/$backupDay");
                $cpcmd = "nice -n 19 ionice -c 3 -n7 cp -ralf " . $job->dest . "/current/* " . $job->dest . "/$backupDay/";
                print "$cpcmd\n";
                system($cpcmd);
                $findcmd = "nice -n 19 ionice -c 3 -n7 find " . $job->dest . "/$backupDay/ -type l -delete";
                print "$findcmd\n";
                $rc = system($findcmd);
            }

            $cmd .= $job->dest . "/$backupDay";

            for ($i=0; $i < 3; $i++)
            {
                print "$cmd\n";
                $rc = system($cmd);
            }

            if ($rc == 0 && file_exists($job->dest . "/$backupDay"))
            {
                $cmd = "ln -nfs $backupDay " . $job->dest . "/current";
                print "$cmd\n";
                if ($p = popen($cmd, "r"))
                {
                    while (fgets($p, 1024))
                    {
                        // TODO: deal with these errors, so subsequent pass will succeed
                        // cannot delete non-empty directory: home/jallen/.rvm/wrappers/ruby-1.9.3-p448@global
                        // could not make way for new symlink: home/jallen/.rvm/wrappers/ruby-1.9.3-p448
                    }

                    pclose($p);
                }
            }

            if ($job->mount != "")
            {
                $cmd = "mount -oremount,ro " . $job->mount;
                print "$cmd\n";
                system($cmd);
            }
        }
    }

    for ($i=1; $i < $argc; $i++)
    {
        $opt = $argv[$i];

        if ($opt == "--backup")
        {
        }
        else
        {
            $rsyncOptions .= " $opt";
        }
    }

    /**
     *
      */
    if (lock())
    {
        print date("d-m-Y G:i", time()) . "\n";
        $config = getConfig();

        backup($config);

        unlock();

        system("sync");
        print date("d-m-Y G:i", time()) . "\n";
    }
    else
    {
        print "gbackup is already running\n";
        exit(3);
    }
?>
