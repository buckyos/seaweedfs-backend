use std::collections::LinkedList;
use std::ops::Range;
use std::fmt::Debug;

pub trait IntervalValue: Clone + Debug {
    fn set_range(&mut self, old_range: Range<i64>, new_range: Range<i64>);
}

impl IntervalValue for () {
    fn set_range(&mut self, _old_range: Range<i64>, _new_range: Range<i64>) {
        // do nothing
    }
}

#[derive(Clone, Debug)]
pub struct Interval<T: IntervalValue> {
    pub range: Range<i64>,
    pub ts_ns: i64,
    pub value: T,
}

#[derive(Clone, Debug)]
pub struct IntervalList<T: IntervalValue> {
    inner: LinkedList<Interval<T>>,
}

impl<T: IntervalValue> FromIterator<Interval<T>> for IntervalList<T> {
    fn from_iter<I: IntoIterator<Item = Interval<T>>>(iter: I) -> Self {
        Self {
            inner: iter.into_iter().collect(),
        }
    }
}

impl<'a, T: IntervalValue> IntoIterator for &'a IntervalList<T> {
    type Item = &'a Interval<T>;
    type IntoIter = std::collections::linked_list::Iter<'a, Interval<T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

impl<'a, T: IntervalValue> IntoIterator for &'a mut IntervalList<T> {
    type Item = &'a mut Interval<T>;
    type IntoIter = std::collections::linked_list::IterMut<'a, Interval<T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter_mut()
    }
}

impl<T: IntervalValue> IntervalList<T> {
    pub fn new() -> Self {
        Self {
            inner: LinkedList::new(),
        }
    }

    pub fn front(&self) -> Option<&Interval<T>> {
        self.inner.front()
    }

    pub fn push_back(&mut self, interval: Interval<T>) {
        self.inner.push_back(interval);
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn overlay(&mut self, interval: Interval<T>) {
        let mut new_list = LinkedList::new();
        let mut rest = self.inner.split_off(0);

        while let Some(current) = rest.pop_front() {
            if current.range.end > interval.range.start {
                if current.range.start < interval.range.start {
                    // 左侧分割
                    let mut left = current.clone();
                    let old_range = left.range.clone();
                    left.range = current.range.start..interval.range.start;
                    left.value.set_range(old_range, left.range.clone());
                    new_list.push_back(left);
                }
                
                if current.range.end > interval.range.end {
                    // 右侧分割
                    let mut right = current.clone();
                    let old_range = right.range.clone();
                    right.range = interval.range.end..current.range.end;
                    right.value.set_range(old_range, right.range.clone());
                    new_list.push_back(interval.clone());
                    new_list.push_back(right);
                    new_list.append(&mut rest);
                    self.inner = new_list;
                    return;
                }
                break;
            }
            new_list.push_back(current);
        }

        new_list.push_back(interval);
        new_list.append(&mut rest);
        self.inner = new_list;
    }

    pub fn insert(&mut self, interval: Interval<T>) {
        let mut new_list = LinkedList::new();
        let mut rest = self.inner.split_off(0);
        let mut current_interval = interval;

        while let Some(current) = rest.pop_front() {
            if current_interval.range.end <= current.range.start {
                new_list.push_back(current_interval);
                new_list.push_back(current);
                new_list.append(&mut rest);
                self.inner = new_list;
                return;
            }

            if current.range.end <= current_interval.range.start {
                new_list.push_back(current);
                continue;
            }

            // 时间戳更新的区间优先
            if current_interval.ts_ns >= current.ts_ns {
                if current.range.start < current_interval.range.start {
                    let mut left = current.clone();
                    let old_range = left.range.clone();
                    left.range = current.range.start..current_interval.range.start;
                    left.value.set_range(old_range, left.range.clone());
                    new_list.push_back(left);
                }
                
                // 添加当前区间
                new_list.push_back(current_interval.clone());
                
                if current_interval.range.end < current.range.end {
                    let mut right = current.clone();
                    let old_range = right.range.clone();
                    right.range = current_interval.range.end..current.range.end;
                    right.value.set_range(old_range, right.range.clone());
                    new_list.push_back(right);
                }
                new_list.append(&mut rest);
                self.inner = new_list;
                return;
            } else {
                if current_interval.range.start < current.range.start {
                    let mut left = current_interval.clone();
                    let old_range = left.range.clone();
                    left.range = current_interval.range.start..current.range.start;
                    left.value.set_range(old_range, left.range.clone());
                    new_list.push_back(left);
                }
                let current_end = current.range.end;  // 先保存 end 值
                new_list.push_back(current);
                if current_end < current_interval.range.end {
                    current_interval.range = current_end..current_interval.range.end;
                    continue;
                }
                self.inner = new_list;
                return;
            }
        }

        new_list.push_back(current_interval);
        new_list.append(&mut rest);
        self.inner = new_list;
    }

    pub fn iter(&self) -> std::collections::linked_list::Iter<'_, Interval<T>> {
        self.inner.iter()
    }

    pub fn iter_mut(&mut self) -> std::collections::linked_list::IterMut<'_, Interval<T>> {
        self.inner.iter_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug)]
    struct IntervalInt(i32);

    impl IntervalValue for IntervalInt {
        fn set_range(&mut self, _old_range: Range<i64>, _new_range: Range<i64>) {}
    }

    #[test]
    fn test_overlay() {
        let mut list = IntervalList::new();
        list.overlay(Interval { range: 0..100, ts_ns: 1, value: IntervalInt(1) });
        list.overlay(Interval { range: 50..150, ts_ns: 2, value: IntervalInt(2) });
        list.overlay(Interval { range: 200..250, ts_ns: 3, value: IntervalInt(3) });
        list.overlay(Interval { range: 225..250, ts_ns: 4, value: IntervalInt(4) });
        list.overlay(Interval { range: 175..210, ts_ns: 5, value: IntervalInt(5) });
        list.overlay(Interval { range: 0..25, ts_ns: 6, value: IntervalInt(6) });
        
        assert_eq!(list.len(), 6);

        list.overlay(Interval { range: 50..150, ts_ns: 7, value: IntervalInt(7) });
        assert_eq!(list.len(), 6);
    }

    #[test]
    fn test_overlay_2() {
        let mut list = IntervalList::new();
        list.overlay(Interval { range: 50..100, ts_ns: 1, value: IntervalInt(1) });
        list.overlay(Interval { range: 0..50, ts_ns: 2, value: IntervalInt(2) });
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_overlay_3() {
        let mut list = IntervalList::new();
        list.overlay(Interval { range: 50..100, ts_ns: 1, value: IntervalInt(1) });
        assert_eq!(list.len(), 1);

        list.overlay(Interval { range: 0..60, ts_ns: 2, value: IntervalInt(2) });
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_overlay_4() {
        let mut list = IntervalList::new();
        list.overlay(Interval { range: 50..100, ts_ns: 1, value: IntervalInt(1) });
        list.overlay(Interval { range: 0..100, ts_ns: 2, value: IntervalInt(2) });
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn test_overlay_5() {
        let mut list = IntervalList::new();
        list.overlay(Interval { range: 50..100, ts_ns: 1, value: IntervalInt(1) });
        list.overlay(Interval { range: 0..110, ts_ns: 2, value: IntervalInt(2) });
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn test_insert_interval_1() {
        let mut list = IntervalList::new();
        list.insert(Interval { range: 50..150, ts_ns: 2, value: IntervalInt(2) });
        list.insert(Interval { range: 200..250, ts_ns: 3, value: IntervalInt(3) });
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_insert_interval_2() {
        let mut list = IntervalList::new();
        list.insert(Interval { range: 50..150, ts_ns: 2, value: IntervalInt(2) });
        list.insert(Interval { range: 0..25, ts_ns: 3, value: IntervalInt(3) });
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_insert_interval_3() {
        let mut list = IntervalList::new();
        list.insert(Interval { range: 50..150, ts_ns: 2, value: IntervalInt(2) });
        list.insert(Interval { range: 200..250, ts_ns: 4, value: IntervalInt(4) });
        list.insert(Interval { range: 0..75, ts_ns: 3, value: IntervalInt(3) });
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn test_insert_interval_4() {
        let mut list = IntervalList::new();
        list.insert(Interval { range: 200..250, ts_ns: 4, value: IntervalInt(4) });
        list.insert(Interval { range: 0..225, ts_ns: 3, value: IntervalInt(3) });
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_insert_interval_6() {
        let mut list = IntervalList::new();
        list.insert(Interval { range: 50..150, ts_ns: 2, value: IntervalInt(2) });
        list.insert(Interval { range: 200..250, ts_ns: 4, value: IntervalInt(4) });
        list.insert(Interval { range: 0..275, ts_ns: 1, value: IntervalInt(1) });
        assert_eq!(list.len(), 5);
    }

    #[test]
    fn test_insert_interval_10() {
        let mut list = IntervalList::new();
        list.insert(Interval { range: 50..100, ts_ns: 2, value: IntervalInt(2) });
        list.insert(Interval { range: 200..300, ts_ns: 4, value: IntervalInt(4) });
        list.insert(Interval { range: 100..200, ts_ns: 5, value: IntervalInt(5) });
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn test_insert_interval_11() {
        let mut list = IntervalList::new();
        list.insert(Interval { range: 0..64, ts_ns: 1, value: IntervalInt(1) });
        list.insert(Interval { range: 72..136, ts_ns: 3, value: IntervalInt(3) });
        list.insert(Interval { range: 64..128, ts_ns: 2, value: IntervalInt(2) });
        list.insert(Interval { range: 68..72, ts_ns: 4, value: IntervalInt(4) });
        assert_eq!(list.len(), 4);
    }

    #[derive(Clone, Debug)]
    struct IntervalStruct {
        x: i32,
        start: i64,
        stop: i64,
    }

    impl IntervalValue for IntervalStruct {
        fn set_range(&mut self, _old_range: Range<i64>, new_range: Range<i64>) {
            self.start = new_range.start;
            self.stop = new_range.end;
        }
    }

    fn new_interval_struct(i: i32) -> IntervalStruct {
        IntervalStruct {
            x: i,
            start: 0,
            stop: 0,
        }
    }

    #[test]
    fn test_insert_interval_struct() {
        let mut list = IntervalList::new();
        list.insert(Interval { range: 0..64, ts_ns: 1, value: new_interval_struct(1) });
        list.insert(Interval { range: 64..72, ts_ns: 2, value: new_interval_struct(2) });
        list.insert(Interval { range: 72..136, ts_ns: 3, value: new_interval_struct(3) });
        list.insert(Interval { range: 64..68, ts_ns: 4, value: new_interval_struct(4) });
        assert_eq!(list.len(), 4);
    }
}
