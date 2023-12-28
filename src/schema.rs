pub struct Schema {
    names: Vec<String>,
    types: Vec<AttributeType>,
    offsets: Vec<usize>,
    lengths: Vec<usize>,
}

impl Schema {
    pub fn new(name_type_list: Vec<(String, AttributeType)>) -> Schema {
        let names = name_type_list.iter().map(|(name, _)| name.clone()).collect();
        let types = name_type_list.iter().map(|(_, typ)| typ.clone()).collect::<Vec<AttributeType>>();
        let lengths = types.iter().map(|typ| typ.get_size()).collect::<Vec<usize>>();
        let mut offsets = Vec::new();
        for i in 0..types.len() {
            if i == 0 {
                offsets.push(0);
            } else {
                offsets.push(offsets[i-1] + lengths[i-1]);
            }
        }
        Schema {
            names,
            types,
            offsets,
            lengths,
        }
    }
}

#[derive(Clone, Debug)]
pub enum AttributeType {
    Int,
    Varchar(usize),
}

impl AttributeType {
    pub fn get_size(&self) -> usize {
        match self {
            AttributeType::Int => 4,
            AttributeType::Varchar(length) => *length,
        }
    }
}

