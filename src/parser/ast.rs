// データ型を表す列挙型
#[derive(Debug, Clone)]
pub enum DataType {
    Int,
    Varchar(usize), // Varcharの場合はサイズを持つ
}

// カラム定義を表す構造体
#[derive(Debug)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

// CREATE TABLE文を表す構造体
#[derive(Debug)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

// INSERT INTO文の値を表す列挙型
#[derive(Debug, Clone)]
pub enum Value {
    Int(i32),
    String(String),
}

// INSERT INTO文を表す構造体
#[derive(Debug)]
pub struct InsertIntoStatement {
    pub table_name: String,
    pub values: Vec<Value>,
}

// SELECT文を表す構造体
#[derive(Debug)]
pub struct SelectStatement {
    pub table_name: String,
    pub columns: Vec<String>, // 選択するカラムのリスト
}

// SQL文全体を表す列挙型
#[derive(Debug)]
pub enum SqlStatement {
    CreateTable(CreateTableStatement),
    InsertInto(InsertIntoStatement),
    Select(SelectStatement),
}
