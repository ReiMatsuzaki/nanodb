mod types;
mod converter;
mod page;
mod diskmgr;
mod bufmgr;
mod filemgr;
mod relmgr;

use crate::diskmgr::run_diskmgr;
use crate::bufmgr::run_bufmgr;
use crate::filemgr::run_hfilemgr;
use crate::filemgr::run_filemgr;
use crate::relmgr::run_relmgr;
use crate::relmgr::run_relmgr_old;

fn main() {
    let i = 5;
    println!("nanodb start");
    if i == 0 {
        run_diskmgr().unwrap();
    } else if i == 1 {
        run_bufmgr().unwrap();
    } else if i == 2 {
        run_filemgr().unwrap();
    } else if i == 3 {
        run_relmgr_old().unwrap();
    } else if i==4 {
        run_hfilemgr().unwrap();
    } else if i==5 {
        run_relmgr().unwrap();
    }
}
