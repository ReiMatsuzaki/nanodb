use std::collections::HashMap;

use crate::buffer_manager::{BufferManager, Record, RecordValue};

use super::types::*;

pub struct Relation {
    name: String,
    file_no: FileNo,
    attributes: Vec<Attribute>,
    // instance: RelationInstance,
}

impl Relation {
    pub fn new(name: String, file_no: FileNo, attributes: Vec<Attribute>) -> Self {
        Self {
            name,
            file_no,
            attributes,
        }
    }

    fn check_record_type(&self, record: &Record) -> Res<()> {
        if record.len() != self.attributes.len() {
            return Err(Error::RecordTypeMismatch);
        }
        for (i, attribute) in self.attributes.iter().enumerate() {
            match (record.get(i), &attribute.ty) {
                (Some(RecordValue::Int(_)), AttributeType::Int) => {},
                (Some(RecordValue::Varchar(_)), AttributeType::Varchar(_)) => {},
                _ => return Err(Error::RecordTypeMismatch),
            }
        }
        Ok(())
    }

    pub fn insert(&mut self, rec: Record, buffer_manager: &mut BufferManager, page_id: PageId) -> Res<()> {
        self.check_record_type(&rec)?;
        let page = buffer_manager.fetch_page(self.file_no, page_id);
        let rid = page.insert(rec)?;
        Ok(())
    }

//     pub fn print(&self, buffer_manager: &mut BufferManager) -> Res<()> {
//         for (page_id, slot_nos) in self.page_slot_nos.iter() {
//             let page = buffer_manager.fetch_page(self.relation.file_no, *page_id);
//             for slot_no in slot_nos.iter() {
//                 let record_id = RecordId::new(*page_id, *slot_no as u32);
//                 let record = page.get(&record_id)?;            
//                 println!("{:?}", record);
//             }
//         }
//         Ok(())
//     }
}

impl std::fmt::Display for Relation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = format!("Relation: {}\n", self.name);
        for attribute in self.attributes.iter() {
            s.push_str(&format!("  {}\n", attribute.name));
        }
        write!(f, "{}", s)
    }
}

#[derive(Clone)]
pub struct Attribute {
    name: String,
    ty: AttributeType,
}

impl Attribute {
    pub fn new_int(name: String) -> Self {
        Self {
            name,
            ty: AttributeType::Int,
        }
    }

    pub fn new_varchar(name: String, size: usize) -> Self {
        Self {
            name,
            ty: AttributeType::Varchar(size),
        }
    }
}

impl AttributeType {
    // pub fn size(&self) -> usize {
    //     match self {
    //         AttributeType::Int => 4,
    //         AttributeType::Varchar(size) => *size,
    //     }
    // }
}

// impl RelationInstance {
//     pub fn new(relation: Relation) -> Self {
//         Self {
//             relation,
//             page_slot_nos: HashMap::new()
//         }
//     }

//     fn check_record_type(&self, record: &Record) -> Res<()> {
//         if record.len() != self.relation.attributes.len() {
//             return Err(Error::RecordTypeMismatch);
//         }
//         for (i, attribute) in self.relation.attributes.iter().enumerate() {
//             match (record.get(i), &attribute.ty) {
//                 (Some(RecordValue::Int(_)), AttributeType::Int) => {},
//                 (Some(RecordValue::Varchar(_)), AttributeType::Varchar(_)) => {},
//                 _ => return Err(Error::RecordTypeMismatch),
//             }
//         }
//         Ok(())
//     }

//     pub fn insert(&mut self, rec: Record, buffer_manager: &mut BufferManager, page_id: PageId) -> Res<()> {
//         self.check_record_type(&rec)?;
//         let page = buffer_manager.fetch_page(self.relation.file_no, page_id);
//         let rid = page.insert(rec)?;
//         if let Some(slot_nos) = self.page_slot_nos.get_mut(&page_id) {
//             slot_nos.push(rid.slot_no as usize);
//         } else {
//             self.page_slot_nos.insert(page_id, vec![rid.slot_no as usize]);
//         }
//         Ok(())
//     }

//     pub fn print(&self, buffer_manager: &mut BufferManager) -> Res<()> {
//         for (page_id, slot_nos) in self.page_slot_nos.iter() {
//             let page = buffer_manager.fetch_page(self.relation.file_no, *page_id);
//             for slot_no in slot_nos.iter() {
//                 let record_id = RecordId::new(*page_id, *slot_no as u32);
//                 let record = page.get(&record_id)?;            
//                 println!("{:?}", record);
//             }
//         }
//         Ok(())
//     }
// }

struct OperatorEvaluator {
    buffer_manager: BufferManager,
    relation_list: Vec<Relation>,
}

impl OperatorEvaluator {
    pub fn new() -> Self {
        Self {
            buffer_manager: BufferManager::new(),
            relation_list: Vec::new(),
        }
    }

    pub fn iter(&self, file_no: FileNo) -> impl Iterator<Item = &Record> {
        self.buffer_manager.record_iterator(file_no)
    }

    pub fn print_relation(&self, file_no: FileNo) -> Res<()> {
        let relation = self.relation_list.iter().find(|r| r.file_no == file_no).unwrap();
        for i in &relation.attributes {
            println!("{}:{:?}", i.name, i.ty);
        }
        // FIXME: print from page
        for record in self.iter(file_no) {
            println!("{:?}", record);
        }
        // self.buffer_manager.(file_no)?;
        Ok(())
    }

    pub fn create_relation(&mut self, name: String, attributes: Vec<Attribute>) -> Res<FileNo> {
        let file_no = self.buffer_manager.create_file();

        let relation = Relation::new(
            name,
            file_no,
            attributes,
        );

        self.relation_list.push(relation);

        Ok(file_no)
    }

    pub fn insert(&mut self, file_no: FileNo, rec: Record, page_id: PageId) -> Res<()> {
        let relation = self.relation_list.iter_mut().find(|r| r.file_no == file_no).unwrap();
        relation.insert(rec, &mut self.buffer_manager, page_id)?;
        Ok(())
    }

    pub fn projection(&mut self, from_file_no: FileNo, pos_list: &Vec<usize>) -> Res<FileNo> {
        let from = self.relation_list.iter().find(|r| r.file_no == from_file_no).unwrap();
        let file_no = self.buffer_manager.create_file();
        let mut relation = Relation::new(
            format!("__temp_{}", file_no.value),
            file_no,
            pos_list.iter().map(|&i| from.attributes[i].clone()).collect(),
        );
        for rec in self.iter(from_file_no) {
            let mut values = Vec::new();
            for &pos in pos_list.iter() {
                values.push(rec.get(pos).unwrap().clone());
            }
            relation.insert(Record::new(values), &mut self.buffer_manager, PageId::new(0))?;
        }
        self.relation_list.push(relation);
        Ok(file_no)
    }



    // pub fn sort(&mut self, rel: &mut RelationInstance, pos: usize) -> Res<()> {
    //     let buffer_manager = &mut self.buffer_manager;
    //     // sort values for each page
    //     for (page_id, slot_nos) in rel.page_slot_nos.iter() {
    //         let page = buffer_manager.fetch_page(rel.relation.file_no, *page_id);
    //         for slot_no in slot_nos.iter() {
    //             let record_id = RecordId::new(*page_id, *slot_no as u32);
    //             let record = page.get(&record_id)?;
    //             let key = record.get_int(pos)?;
    //         }
    //         // page.sort(pos);
    //     }
    //     Ok(())
    // }
}

pub fn run_query_evaluator() -> Res<()>{
    let mut evaluator = OperatorEvaluator::new();
    // let buffer_manager = &mut evaluator.buffer_manager;
    let fno = evaluator.create_relation(
        "test".to_string(),
        vec![
            Attribute::new_int("id".to_string()),
            Attribute::new_varchar("name".to_string(), 5),
        ],
    )?;

    evaluator.insert(fno, Record::new(vec![
        RecordValue::Int(1),
        RecordValue::Varchar("ab".to_string()),
    ]), PageId::new(0))?;
    evaluator.insert(fno, Record::new(vec![
        RecordValue::Int(3),
        RecordValue::Varchar("cd".to_string()),
    ]), PageId::new(1))?;
    evaluator.insert(fno, Record::new(vec![
        RecordValue::Int(2),
        RecordValue::Varchar("ef".to_string()),
    ]), PageId::new(1))?;

    evaluator.print_relation(fno)?;

    // evaluator.sort(&mut instance, 0)?;

    // let instance2 = evaluator.projection(&instance, &vec![0])?;

    // instance2.print(buffer_manager)?;

    // let buffer_manager = &mut evaluator.buffer_manager;
    // instance.print(buffer_manager)?;
    Ok(())
}