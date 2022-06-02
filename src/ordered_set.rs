use std::collections::HashSet;
use std::hash::Hash;
use std::ops::Deref;

pub struct OrderedSet<T>
where T: Clone + Hash + Ord
{
    names: Vec<T>,
    set: HashSet<T>,
}

impl <T> OrderedSet<T>
where T: Clone + Hash + Ord {
    pub fn new() -> Self {
        Self{names: Vec::new(), set: HashSet::new()}
    }

    pub fn add(&mut self, name: T) {
        if !self.set.contains(&name) {
            let idx = self.names.binary_search(&name)
                                .unwrap_or_else(|idx| idx);
            self.names.insert(idx, name.clone());
            self.set.insert(name);
        }
    }

    pub fn remove(&mut self, name: &T) {
        if self.set.remove(name) {
            let idx = self.names.binary_search(name).unwrap();
            self.names.remove(idx);
        }
    }
}

impl <T> Deref for OrderedSet<T>
where T: Clone + Hash + Ord {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.names
    }
}
