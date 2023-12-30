use std::sync::{Arc, Mutex};

use crate::relop::{AttributeType, Projection, FileScan};
use crate::types::*;

use crate::diskmgr::DiskMgr;
use crate::bufmgr::BufMgr;
use crate::filemgr::{HFileMgr, HeapFile};
use crate::relop::{schema::Schema, Record};

use crate::parser::*;

const CATALOG_ATTRIBUTE_CAT: &str = "attr_";

pub struct NanoDb {
    filemgr: HFileMgr,

    catalog_attr_cat_file: Arc<Mutex<HeapFile>>,
    catalog_attr_cat_schema: Schema,
}

impl NanoDb {
    pub fn build(name: &str) -> Res<NanoDb> {
        let diskmgr = DiskMgr::open_db(name)?;
        let bufmgr = BufMgr::new(10, diskmgr);
        let bufmgr = Arc::new(Mutex::new(bufmgr));
        let mut filemgr = HFileMgr::build(bufmgr)?;

        let catalog_attr_cat_file = filemgr.open(CATALOG_ATTRIBUTE_CAT)?;
        let catalog_attr_cat_file = Arc::new(Mutex::new(catalog_attr_cat_file));
        let catalog_attr_cat_schema = Schema::build(vec![
            ("aname".to_string(), AttributeType::Varchar(10) ),
            ("rname".to_string(), AttributeType::Varchar(10) ),
            ("type_".to_string(), AttributeType::Varchar(10) ),
            ("size".to_string(), AttributeType::Int ),
            ("posit".to_string(), AttributeType::Int ),
        ]);
        Ok(NanoDb{ filemgr, catalog_attr_cat_file, catalog_attr_cat_schema })
    }

    pub fn init(&mut self) -> Res<()> {
        // let a = &self.catalog_attr_cat_schema;
        // self.insert_into_schema(a, CATALOG_ATTRIBUTE_CAT)?;
        // FIXME: check existance before write
        let flen = self.catalog_attr_cat_schema.len();
        let rel_name = CATALOG_ATTRIBUTE_CAT;
        for fno in 0..flen {
            let schema = &self.catalog_attr_cat_schema;
            let attr_name = schema.get_name(fno).unwrap().clone();
            let ty = match schema.get_type(fno).unwrap() {
                AttributeType::Int => DataType::Int,
                AttributeType::Varchar(n) => DataType::Varchar(*n),
            };
            self.insert_into_catalog_attr_type(&attr_name, rel_name, ty, fno)?;
        }        
        Ok(())
    }

    pub fn execute_statement(&mut self, statement: SqlStatement) -> Res<()> {
        match statement {
            SqlStatement::CreateTable(s) => {
                self.execute_create_table(s)?;
                Ok(())
            },
            SqlStatement::InsertInto(s) => self.execute_insert_into(s),
            SqlStatement::Select(s) => {
                let mut it = self.execute_select(s)?;
                log::info!("select result:");
                while let Some((rid, rec)) = it.get_next()? {
                    println!("{}: {}", rid, rec);
                }
                Ok(())
            }
        }
    }

    fn execute_create_table(&mut self, statement: CreateTableStatement) -> Res<HeapFile> {
        let heap_file = self.filemgr.create_file(&statement.table_name)?;
        for fno in 0..statement.columns.len() {
            let c = statement.columns.get(fno).unwrap();
            self.insert_into_catalog_attr_type(&c.name, &statement.table_name, 
                c.data_type.clone(), fno)?;
        }
        Ok(heap_file)
    }

    fn insert_into_catalog_attr_type(&mut self, attr_name: &str, rel_name: &str, ty: DataType, fno: usize) -> Res<()> {
        log::debug!("insert_into_catalog_attr_type(attr_name={}, rel_name={}, fno={})",
                attr_name, rel_name, fno);
        let mut rec: Record = Record::new_zero(&self.catalog_attr_cat_schema);
        rec.set_varchar_field(0, &attr_name.to_string())?;
        rec.set_varchar_field(1, &rel_name.to_string())?;
        let (ty, size) = match ty {
            DataType::Int => ("int".to_string(), 1),
            DataType::Varchar(n) => ("varchar".to_string(), n),
        };
        rec.set_varchar_field(2, &ty)?;
        rec.set_int_field(3, size as i32)?;
        rec.set_int_field(4, fno as i32)?;
        let mut mutex = self.catalog_attr_cat_file.lock().unwrap();
        mutex.insert_record(*rec.get_data())?;
        Ok(())
    }

    fn execute_insert_into(&mut self, statement: InsertIntoStatement) -> Res<()> {
        log::debug!("execute_insert_into");
        let (mut file, schema) = self.open_relation(statement.table_name.as_str())?;
        if schema.len() != statement.values.len() {
            return Err(Error::InvalidArg { 
                msg: format!("size mismatch between insertion values ({}) and number of field ({})",
                statement.values.len(),
                schema.len(),
            )
            })
        }

        log::debug!("execute_insert_into: add values to record");
        let mut rec: Record = Record::new_zero(&schema);
        for fno in 0..schema.len() {
            match statement.values.get(fno).unwrap() {
                Value::Int(x) => {
                    rec.set_int_field(fno, *x)?;
                },
                Value::String(x) => {
                    rec.set_varchar_field(fno, x)?;
                }
            }
        }

        log::debug!("execute_insert_into: add record");
        file.insert_record(*rec.get_data())?;
        Ok(())
    }

    fn execute_select(&mut self, statement: SelectStatement) -> Res<Projection> {
        log::debug!("execute_select");
        let (file, schema) = self.open_relation(&statement.table_name)?;
        // println!("schema: {:?}", schema);
        let file = Arc::new(Mutex::new(file));
        let file_scan = FileScan::new(file.clone(), schema.clone());

        let mut fnos = Vec::new();
        for c in statement.columns {
            for fno in 0..schema.len() {
                if &c == schema.get_name(fno).unwrap() {
                    fnos.push(fno);
                    break;
                }
            }
        }
        Projection::build(file_scan, fnos)
    }

    fn open_relation(&mut self, name: &str) -> Res<(HeapFile, Schema)> {
        log::debug!("open_relation");
        // let file = self.filemgr.open(CATALOG_ATTRIBUTE_CAT)?;
        let a = self.catalog_attr_cat_file.clone();
        let s = self.catalog_attr_cat_schema.clone();
        let mut file_scan = FileScan::new(a, s);
        let mut buf = Vec::new();
        while let Some((_, rec)) = file_scan.get_next()? {
            let aname = rec.get_varchar_field(0).unwrap();
            let rname = rec.get_varchar_field(1).unwrap();
            let type_name = rec.get_varchar_field(2).unwrap();
            let type_size = rec.get_int_field(3).unwrap() as usize;
            let position = rec.get_int_field(4).unwrap() as usize;
            if rname.eq(name) {
                let attr_type = AttributeType::decode(type_name.as_str(), type_size).unwrap();
                buf.push((aname, attr_type, position));
            }
        }
        if buf.len() == 0 {
            return Err(Error::RelationNotFound { name: name.to_string() })
        }

        let n = buf.len();
        let mut xs = Vec::new();
        for i in 0..n {
            let x = buf.iter()
            .find(|x| x.2 == i)
            .map(|x| (x.0.clone(), x.1.clone())).unwrap();
            xs.push(x);
        }
        let schema = Schema::build(xs);

        let file = self.filemgr.open(name)?;
        Ok((file, schema))
    }
}

pub fn run_nanodb() -> Res<()> {
    log::info!("run_nanodb start");
    let name = "nano.db";
    let mut nanodb = NanoDb::build(name)?;
    nanodb.init()?;

    log::info!("create student table");
    let statement = CreateTableStatement { 
        table_name: "student".to_string(),
        columns: vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int},
            ColumnDef { name: "name".to_string(), data_type: DataType::Varchar(10)},
            ColumnDef { name: "score".to_string(), data_type: DataType::Int},
        ]};
    let statement = SqlStatement::CreateTable(statement);
    nanodb.execute_statement(statement)?;

    log::info!("insert into");
    for i in 0..10 {
        let statement = InsertIntoStatement { 
            table_name: "student".to_string(),
            values: vec![
                Value::Int(3+i),
                Value::String(format!("MyName{}", i)),
                Value::Int(80+i),
            ]
        };
        let statement = SqlStatement::InsertInto(statement);
        nanodb.execute_statement(statement)?;
    }

    log::info!("select catalog");
    let statement = SelectStatement {
        table_name: CATALOG_ATTRIBUTE_CAT.to_string(),
        columns: vec!["aname".to_string(), "rname".to_string(), "type_".to_string()],
    };
    let statement = SqlStatement::Select(statement);
    nanodb.execute_statement(statement)?;

    log::info!("select student table");
    let statement = SelectStatement {
        table_name: "student".to_string(),
        columns: vec!["id".to_string(), "score".to_string()],
    };
    let statement = SqlStatement::Select(statement);
    nanodb.execute_statement(statement)?;

    std::fs::remove_file(name).unwrap();
    Ok(())
}