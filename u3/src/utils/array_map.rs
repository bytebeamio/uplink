use std::cmp::Ordering;

pub struct ArrayMap<K, V> {
    items: Vec<(K, V)>,
    compare: fn(&V, &V) -> Ordering,
}

impl<K, V> ArrayMap<K, V> {
    pub fn new(compare: fn(&V, &V) -> Ordering) -> Self {
        ArrayMap { items: Vec::new(), compare }
    }

    pub fn insert(&mut self, key: K, value: V) {
        let insert_pos =
            self.items.binary_search_by(|(_, v)| (self.compare)(v, &value)).unwrap_or_else(|e| e);
        self.items.insert(insert_pos, (key, value));
    }

    pub fn get(&self, key: &K) -> Option<&V>
    where
        K: PartialEq,
    {
        self.items.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V>
    where
        K: PartialEq,
    {
        self.items.iter_mut().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    pub fn contains_key(&self, key: &K) -> bool
    where
        K: PartialEq,
    {
        self.get(key).is_some()
    }

    pub fn iter(&self) -> impl Iterator<Item = &(K, V)> {
        self.items.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut (K, V)> {
        self.items.iter_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_array_map() {
        let mut map = ArrayMap::new(|a: &i32, b: &i32| a.cmp(b));

        map.insert("one", 1);
        map.insert("two", 2);
        map.insert("three", 3);
        map.insert("zero", 0);

        let values: Vec<i32> = map.iter().map(|(_, v)| *v).collect();
        assert_eq!(values, vec![0, 1, 2, 3]);

        assert_eq!(map.get(&"two"), Some(&2));
        assert_eq!(map.get(&"four"), None);

        map.iter_mut().for_each(|(_, v)| *v += 1);
        let values: Vec<i32> = map.iter().map(|(_, v)| *v).collect();
        assert_eq!(values, vec![1, 2, 3, 4]);
    }
}
