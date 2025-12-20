pub mod backup;
pub mod disk_add;
pub mod mount;
pub mod umount;

use crate::error::{DiskError, TimevaultError};

pub fn exit_for_disk_error(err: &DiskError) -> ! {
    let code = match err {
        DiskError::NoDiskConnected => 10,
        DiskError::MultipleDisksConnected => 11,
        DiskError::IdentityMismatch(_) => 12,
        DiskError::DiskNotEmpty(_) => 13,
        DiskError::MountFailure(_) | DiskError::UmountFailure(_) => 14,
        DiskError::Other(_) => 2,
    };
    println!("{}", err);
    std::process::exit(code);
}

pub fn exit_for_error(err: &TimevaultError) -> ! {
    match err {
        TimevaultError::Disk(disk) => exit_for_disk_error(disk),
        _ => {
            println!("{}", err);
            std::process::exit(2);
        }
    }
}
