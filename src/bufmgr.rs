use super::types::{Res, Error, PageId};
use super::page::Page;
use super::diskmgr::DiskMgr;

pub struct Frame {
    page_id: PageId,
    pin_count: usize,
    dirty: bool,
    page: Page
}

impl Frame {
    pub fn new(page_id: PageId, page: Page) -> Self {
        Self {
            page_id,
            pin_count: 0,
            dirty: false,
            page,
        }
    }

    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    pub fn get_pin_count(&self) -> usize {
        self.pin_count
    }

    pub fn get_dirty(&self) -> bool {
        self.dirty
    }

    // pub fn get_page(&self) -> &Page {
    //     &self.page
    // }

    // pub fn get_page_mut(&mut self) -> &mut Page {
    //     &mut self.page
    // }

    // pub fn set_page(&mut self, page: Page) {
    //     self.page = page;
    // }

    pub fn set_dirty(&mut self, dirty: bool) {
        self.dirty = dirty;
    }

    pub fn pin(&mut self) {
        self.pin_count += 1;
    }

    pub fn unpin(&mut self) {
        self.pin_count -= 1;
    }

}

pub struct BufMgr {
    buf_pool: Vec<Frame>,
    max_bufsize: usize,
    diskmgr: DiskMgr,
}

impl BufMgr {
    pub fn new(max_bufsize: usize, diskmgr: DiskMgr) -> Self {
        let buf_pool = Vec::new();
        // for _ in 0..max_bufsize {
        //     buf_pool.push(Frame::new());
        // }
        Self {
            buf_pool,
            max_bufsize,
            diskmgr,
        }
    }

    // fn get_frame_mut(&mut self, page_id: PageId) -> Res<&mut Frame> {
    //     for frame in self.buf_pool.iter_mut() {
    //         if frame.get_page_id() == page_id {
    //             return Ok(frame);
    //         }
    //     }
    //     Err(Error::InvalidArg{ msg: format!("page_id {} is not found", page_id)})
    // }

    fn get_frame_index(&self, page_id: PageId) -> Option<usize> {
        for (i, frame) in self.buf_pool.iter().enumerate() {
            if frame.get_page_id() == page_id {
                return Some(i);
            }
        }
        None
    }

    pub fn pin_page(&mut self, page_id: PageId) -> Res<&mut Page> {
        match self.get_frame_index(page_id) {
            Some(idx) => {
                let frame = self.buf_pool.get_mut(idx).unwrap();
                frame.pin();
                Ok(&mut frame.page)
            },
            None if self.buf_pool.len() < self.max_bufsize => {
                let page = self.diskmgr.read_page(page_id)?;
                let mut frame = Frame::new(page_id, page);
                frame.pin();
                self.buf_pool.push(frame);
                Ok(&mut self.buf_pool.last_mut().unwrap().page)
            }
            None => panic!("not implemented for buf_pool full"),
        }
    }

    pub fn unpin_page(&mut self, page_id: PageId) -> Res<()> {
        match self.get_frame_index(page_id) {
            Some(idx) => {
                let frame = self.buf_pool.get_mut(idx).unwrap();
                frame.unpin();
                Ok(())
            },
            None => Err(Error::PageNotFound { page_id: page_id, msg: "page_id not found for unpin_page".to_string() })
        }
    }

    pub fn create_page(&mut self) -> Res<(PageId, &mut Page)> {
        let page_id = self.diskmgr.allocate_page()?;
        let page = self.pin_page(page_id)?;
        Ok((page_id, page))
    }

    pub fn flush_page(&mut self, page_id: PageId) -> Res<()> {
        if let Some(idx) = self.get_frame_index(page_id) {
            let frame = self.buf_pool.get_mut(idx).unwrap();
            if frame.get_dirty() {
                self.diskmgr.write_page(page_id, &frame.page)?;
                frame.set_dirty(false);
            }
            Ok(())
        } else {
            Err(Error::PageNotFound { page_id: page_id, msg: "page_id not found for flush_page".to_string() })
        }

    }

    pub fn free_page(&mut self, page_id: PageId) -> Res<()> {
        match self.get_frame_index(page_id) {
            Some(idx) => {
                let frame = self.buf_pool.get_mut(idx).unwrap();
                if frame.get_pin_count() > 0 {
                    return Err(Error::InvalidArg{ msg: format!("page is pinned. pid={} pin_count={}", page_id, frame.get_pin_count())});
                }
                self.diskmgr.deallocate_page(page_id)?;
                self.buf_pool.remove(idx);
            },
            None => {
                self.diskmgr.deallocate_page(page_id)?;
            }
        }
        Ok(())
    }
}

pub fn run_bufmgr() -> Res<()> {
    let diskmgr = DiskMgr::open_db("nano.db").unwrap();
    let mut bufmgr = BufMgr::new(10, diskmgr);
    let (pid0, page0) = bufmgr.create_page().unwrap();

    page0.set_varchar_value(10, "written by bufmgr")?;
    bufmgr.flush_page(pid0)?;
    bufmgr.unpin_page(pid0)?;

    let page1 = bufmgr.pin_page(pid0)?;
    assert_eq!(page1.get_varchar_value(10, 17)?, "written by bufmgr");
    bufmgr.unpin_page(pid0)?;

    bufmgr.free_page(pid0)?;
    let e = bufmgr.pin_page(pid0);
    assert!(e.is_err());

    // remove nano.db file
    std::fs::remove_file("nano.db").unwrap();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bufmgr() {
        run_bufmgr().unwrap();
    }
}