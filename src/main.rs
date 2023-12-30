mod types;
mod converter;
mod page;
mod diskmgr;
mod bufmgr;
mod filemgr;
mod relop;
mod parser;
mod nanodb;

use env_logger;

use crate::diskmgr::run_diskmgr;
use crate::bufmgr::run_bufmgr;
use crate::filemgr::run_hfilemgr;
use crate::relop::{run_relmgr, run_relmgr_projection};
use crate::nanodb::run_nanodb;

fn main() {
    env_logger::init();

    let i = 10;
    println!("nanodb start");
    if i == 0 {
        run_diskmgr().unwrap();
    } else if i == 1 {
        run_bufmgr().unwrap();
    } else if i==4 {
        run_hfilemgr().unwrap();
    } else if i==5 {
        run_relmgr().unwrap();
    } else if i==6 {
        run_relmgr_projection().unwrap();
    } else if i==10 {
        match run_nanodb() {
            Ok(_) => {},
            Err(e) => {
                log::error!("error:\n{:?}", e);
            }
        }
    }
}
