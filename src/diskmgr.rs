use std::fs::File;
use std::io::{Seek, SeekFrom, Read, Write};

use super::types::*;
use super::page::{Page, PAGE_BYTE};

pub struct DiskMgr {
    name: String,
    fp: File,
    // bitmap_page: Page,
}

const NUM_PAGES: usize = 10; // each file has 10 pages
const BITMAP_PAGE_ID: PageId = 0; // the first page is bitmap page
const FIRST_PAGE_ID: PageId = 1;

impl DiskMgr {
    // pub fn new(name: String) -> Self {
    //     Self {
    //         name,
    //     }
    // }

    pub fn open_db(name: &str) -> Res<DiskMgr> {
        let fp = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(name)
            .and_then(|f|{
                f.set_len((PAGE_BYTE * NUM_PAGES) as u64)?;
                Ok(f)
            })
            .map_err(|e| Error::IoError(e))?;
        let diskmgr = DiskMgr {
            name: name.to_string(),
            fp,
            // bitmap_page,
        };
        Ok(diskmgr)
    }

    pub fn close_db(&mut self) -> Res<()> {
        self.fp.sync_all().map_err(|e| Error::IoError(e))?;
        Ok(())
    }

    fn read_page_no_check(fp: &mut File, page_id: PageId) -> Res<Page> {
        let mut page = Page::new();
        // let mut buf = vec![0; PAGE_BYTE];
        fp.seek(SeekFrom::Start((page_id * PAGE_BYTE) as u64))
        .and_then(|_| fp.read_exact(&mut page.get_data_mut()))
        .map_err(|e| Error::IoError(e))?;
        // page.set_data(&buf)?;
        Ok(page)        
    }

    fn read_bitmap_page(&mut self) -> Res<Page> {
        let mut page = Page::new();
        // let mut buf = vec![0; PAGE_BYTE];
        self.fp.seek(SeekFrom::Start(BITMAP_PAGE_ID as u64 * PAGE_BYTE as u64))
        .and_then(|_| self.fp.read_exact(&mut page.get_data_mut()))
        .map_err(|e| Error::IoError(e))?;
        // page.set_data(&buf)?;
        Ok(page)        
    }

    fn write_bitmap_page(&mut self, page: &Page) -> Res<()> {
        // let buf = page.get_data();
        self.fp.seek(SeekFrom::Start(0))
        .and_then(|_| self.fp.write_all(&page.get_data()))
        .map_err(|e| Error::IoError(e))?;
        Ok(())
    }

    pub fn read_page(&mut self, page_no: PageId) -> Res<Page> {
        if self.is_free_page(page_no)? {
            return Err(Error::InvalidArg { msg: "not allocated page".to_string() });
        }
        Self::read_page_no_check(&mut self.fp, page_no)
    }

    pub fn write_page(&mut self, page_no: PageId, page: &Page) -> Res<()> {
        if self.is_free_page(page_no)? {
            return Err(Error::InvalidArg { msg: "not allocated page".to_string() });
        }
        // let buf = page.get_data();
        self.fp.seek(SeekFrom::Start((page_no * PAGE_BYTE) as u64))
        .and_then(|_| self.fp.write_all(&page.get_data()))
        .map_err(|e| Error::IoError(e))?;
        Ok(())
    }

    pub fn is_free_page(&mut self, page_id: PageId) -> Res<bool> {
        let bitmap_page = self.read_bitmap_page()?;
        let byte = bitmap_page.get_byte_value(page_id)?;
        Ok(byte == 0)
    }

    pub fn set_page_type(&mut self, page_id: PageId, page_type: u8) -> Res<()> {
        let mut bitmap_page = self.read_bitmap_page()?;
        let byte = bitmap_page.get_byte_value(page_id)?;
        if byte == page_type {
            return Ok(());
        }
        bitmap_page.set_byte_value(page_id, page_type)?;
        self.write_bitmap_page(&bitmap_page)?;
        Ok(())
    }

    pub fn allocate_page(&mut self) -> Res<PageId> {
        let bitmap_page = self.read_bitmap_page()?;
        for pid in FIRST_PAGE_ID..NUM_PAGES {
            if bitmap_page.get_byte_value(pid)? == 0 {
                self.set_page_type(pid, 1)?;
                return Ok(pid);
            }
        }
        Err(Error::NoFreePage)
    }

    pub fn deallocate_page(&mut self, page_id: PageId) -> Res<()> {
        self.set_page_type(page_id, 0)
    }
}

pub fn run_diskmgr() -> Res<()> {
    let name = "nano-diskmgr-test.db";
    let mut diskmgr = DiskMgr::open_db(name)?;
    println!("diskmgr: {}", diskmgr.name);
    let pid0 = diskmgr.allocate_page()?;
    let pid1 = diskmgr.allocate_page()?;
    let mut page0 = diskmgr.read_page(pid0)?;
    let mut page1 = diskmgr.read_page(pid1)?;
    page0.set_varchar_value(10, "hello")?;
    page1.set_int_value(6, 9)?;
    diskmgr.write_page(pid0, &page0)?;
    diskmgr.write_page(pid1, &page1)?;

    let page2 = diskmgr.read_page(pid0)?;
    let page3 = diskmgr.read_page(pid1)?;
    assert_eq!("hello", page2.get_varchar_value(10, 5)?);
    assert_eq!(9, page3.get_int_value(6)?);

    diskmgr.deallocate_page(pid0)?;
    diskmgr.deallocate_page(pid1)?;

    let a = diskmgr.read_page(pid0);
    // println!("{:?}", a.err());
    assert!(a.is_err());

    diskmgr.close_db()?;

    // remove nano.db file
    std::fs::remove_file(name).unwrap();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_diskmgr() {
        run_diskmgr().unwrap();
    }
}
