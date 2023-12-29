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
use crate::relmgr::run_relmgr;

fn main() {
    let i = 5;
    println!("nanodb start");
    if i == 0 {
        run_diskmgr().unwrap();
    } else if i == 1 {
        run_bufmgr().unwrap();
    } else if i==4 {
        run_hfilemgr().unwrap();
    } else if i==5 {
        run_relmgr().unwrap();
    }
}
