use std::collections::LinkedList;
use std::ops::Range;

pub trait IntervalValue: Clone {
    fn set_range(&mut self, range: Range<i64>);
}

#[derive(Clone)]
pub struct Interval<T: IntervalValue> {
    pub range: Range<i64>,
    pub ts_ns: i64,
    pub value: T,
}

pub struct IntervalList<T: IntervalValue> {
    inner: LinkedList<Interval<T>>,
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
                    left.range = current.range.start..interval.range.start;
                    left.value.set_range(left.range.clone());
                    new_list.push_back(left);
                }
                
                if current.range.end > interval.range.end {
                    // 右侧分割
                    let mut right = current.clone();
                    right.range = interval.range.end..current.range.end;
                    right.value.set_range(right.range.clone());
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

            if current_interval.ts_ns >= current.ts_ns {
                if current.range.start < current_interval.range.start {
                    let mut left = current.clone();
                    left.range = current.range.start..current_interval.range.start;
                    left.value.set_range(left.range.clone());
                    new_list.push_back(left);
                }
                if current_interval.range.end < current.range.end {
                    let new_range = current_interval.range.end..current.range.end;
                    current_interval.range = new_range;
                    current_interval.value.set_range(current_interval.range.clone());
                    break;
                }
            } else {
                if current_interval.range.start < current.range.start {
                    let mut left = current_interval.clone();
                    left.range = current_interval.range.start..current.range.start;
                    left.value.set_range(left.range.clone());
                    new_list.push_back(left);
                }
                new_list.push_back(current.clone());
                if current.range.end < current_interval.range.end {
                    let new_range = current.range.end..current_interval.range.end;
                    current_interval.range = new_range;
                    current_interval.value.set_range(current_interval.range.clone());
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
}
