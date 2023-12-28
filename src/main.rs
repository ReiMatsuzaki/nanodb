mod types;
mod page;
mod diskmgr;
mod bufmgr;
mod filemgr;
mod relmgr;

use crate::diskmgr::run_diskmgr;
use crate::bufmgr::run_bufmgr;
use crate::filemgr::run_filemgr;
// use crate::relmgr::run_relmgr;

fn main() {
    let i = 2;
    println!("nanodb start");
    // let diskmgr = diskmgr::DiskMgr::open_db("nanodb");
    // run_diskmgr().unwrap();
    if i == 0 {
        run_diskmgr().unwrap();
    } else if i == 1 {
        run_bufmgr().unwrap();
    } else if i == 2 {
        run_filemgr().unwrap();
    }
}
