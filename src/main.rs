use crate::diskmgr::run_diskmgr;

mod types;
// mod buffer_manager;
// mod operator_evaluator;
mod page;
mod diskmgr;

// use buffer_manager::*;
// use operator_evaluator::*;

fn main() {
    // let i = 1;
    println!("nanodb start");
    // let diskmgr = diskmgr::DiskMgr::open_db("nanodb");
    run_diskmgr().unwrap();
    // if i == 0 {
    //     run_buffer_manager().unwrap();
    // } else if i == 1 {
    //     run_query_evaluator().unwrap();
    // }
}
