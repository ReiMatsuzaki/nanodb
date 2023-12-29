#[derive(Debug)]
pub struct Schema {
    names: Vec<String>,
    types: Vec<AttributeType>,
    offsets: Vec<usize>,
    // lengths: Vec<usize>,
}

impl Schema {
    pub fn new(names: Vec<String>, types: Vec<AttributeType>, offsets: Vec<usize>) -> Schema {
        Schema {
            names,
            types,
            offsets,
        }
    }

    pub fn build(name_type_list: Vec<(String, AttributeType)>) -> Schema {
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
        Schema::new(
            names,
            types,
            offsets,
        )
    }

    pub fn get_offset(&self, fno: usize) -> Option<&usize> {
        self.offsets.get(fno)
    }

    pub fn get_type(&self, fno: usize) -> Option<&AttributeType> {
        self.types.get(fno)
    }

    // pub fn get_length(&self, fno: usize) -> Option<&usize> {
    //     self.lengths.get(fno)
    // }

    pub fn get_name(&self, fno: usize) -> Option<&String> {
        self.names.get(fno)
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn projection(&self, fnos: &Vec<usize>) -> Option<Schema> {
        if fnos.iter().all(|fno| *fno < self.len()) {
            // let mut names = Vec::new();
            // let mut types = Vec::new();
            let mut xs = Vec::new();
            // let mut offsets = Vec::new();
            for fno in fnos {
                let fno = *fno;
                xs.push((self.names.get(fno).unwrap().clone(),
                         self.types.get(fno).unwrap().clone()
                ));
            }
            Some(Schema::build(xs))
        } else {
            None
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

impl std::fmt::Display for AttributeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let a = match self {
            AttributeType::Int => "int",
            AttributeType::Varchar(_) => "varchar",
        };
        write!(f, "{}", a)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema() {
        let schema = Schema::build(vec![
            ("id".to_string(), AttributeType::Int),
            ("name".to_string(), AttributeType::Varchar(3)),
            ("qty".to_string(), AttributeType::Int),
        ]);
        let fnos = vec![1, 2];
        let schema = schema.projection(&fnos).unwrap();
        assert_eq!(2, schema.len());
        assert_eq!("name", schema.get_name(0).unwrap());
        assert!(match schema.get_type(1).unwrap() {
            AttributeType::Int => true,
            _ => false,
        });
        println!("{:?}", schema);
    }
}