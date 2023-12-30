// #[derive(PartialEq, Eq, Hash, Clone, Copy)]
// pub struct PageId {
//     pub value: u32,
// }

// impl PageId {
//     pub fn new(value: u32) -> Self {
//         Self { value }
//     }
// }

// #[derive(PartialEq, Eq, Hash, Clone, Copy)]
// pub struct FileNo {
//     pub value: u32,
// }

// impl FileNo {
//     pub fn new(value: u32) -> Self {
//         Self { value }
//     }
// }

// pub struct RecordId {
//     pub page_id: PageId,
//     pub slot_no: u32,

// }

// impl RecordId {
//     pub fn new(page_id: PageId, slot_no: u32) -> Self {
//         Self {
//             page_id,
//             slot_no,
//         }
//     }
// }
pub type PageId = usize;

// #[derive(Clone, Debug)]
// pub enum AttributeType {
//     Int,
//     Varchar(usize),
// }

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct EntryNo {
    pub value: usize,
}
impl EntryNo {
    pub fn new(value: usize) -> Self {
        Self { value }
    }

}

#[derive(Debug)]
pub enum Error {
    IoError(std::io::Error),
    InvalidArg{ msg: String},
    NoFreePage,
    PageNotFound { page_id: PageId, msg: String },
    RelationNotFound { name: String, }
    // EmptyRecord,
    // PageMismatch,
    // RecordTypeMismatch,
}

pub type Res<T> = Result<T, Error>;