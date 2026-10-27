use std::fmt;
use std::mem;
use std::ops::{Index, Range};

use super::{HeaderName, HeaderValue};

/// Header fields: names, each with one or more values in order.
///
/// A flat list scanned in order: a request carries a few dozen fields at most,
/// where a scan beats hashing and nothing can be flooded. Values of the same
/// name are kept next to each other.
#[derive(Clone)]
pub struct HeaderMap<T = HeaderValue> {
    // Entries with the same name are contiguous.
    entries: Vec<(HeaderName, T)>,
}

/// A map holds at most this many values.
pub const MAX_SIZE: usize = 1 << 15;

/// The map already holds [`MAX_SIZE`] values.
#[derive(Debug)]
pub struct MaxSizeReached(());

/// Every value of one name, in order.
pub struct GetAll<'a, T> {
    run: &'a [(HeaderName, T)],
}

/// A key to look a name up with: a [`HeaderName`] or a `str` in any case.
pub trait AsHeaderName: sealed::Lookup {}

/// A key to store a name under: a [`HeaderName`] or a lowercase `&'static str`.
pub trait IntoHeaderName: sealed::Store {}

mod sealed {
    use super::HeaderName;

    pub trait Lookup {
        fn matches(&self, name: &HeaderName) -> bool;
    }

    pub trait Store {
        fn into_name(self) -> HeaderName;
    }

    impl Lookup for HeaderName {
        fn matches(&self, name: &HeaderName) -> bool {
            self == name
        }
    }

    impl Lookup for &HeaderName {
        fn matches(&self, name: &HeaderName) -> bool {
            *self == name
        }
    }

    impl Lookup for &str {
        fn matches(&self, name: &HeaderName) -> bool {
            name == *self
        }
    }

    impl Lookup for String {
        fn matches(&self, name: &HeaderName) -> bool {
            name == self.as_str()
        }
    }

    impl Lookup for &String {
        fn matches(&self, name: &HeaderName) -> bool {
            name == self.as_str()
        }
    }

    impl Store for HeaderName {
        fn into_name(self) -> HeaderName {
            self
        }
    }

    impl Store for &HeaderName {
        fn into_name(self) -> HeaderName {
            self.clone()
        }
    }

    /// Panics unless the name is a lowercase token, like `HeaderName::from_static`.
    impl Store for &'static str {
        fn into_name(self) -> HeaderName {
            HeaderName::from_static(self)
        }
    }
}

use sealed::Lookup;

impl AsHeaderName for HeaderName {}
impl AsHeaderName for &HeaderName {}
impl AsHeaderName for &str {}
impl AsHeaderName for String {}
impl AsHeaderName for &String {}

impl IntoHeaderName for HeaderName {}
impl IntoHeaderName for &HeaderName {}
impl IntoHeaderName for &'static str {}

impl HeaderMap {
    pub fn new() -> HeaderMap {
        HeaderMap::with_capacity(0)
    }
}

impl<T> HeaderMap<T> {
    pub fn with_capacity(capacity: usize) -> HeaderMap<T> {
        HeaderMap {
            entries: Vec::with_capacity(capacity),
        }
    }

    /// Number of values, counting each value of a repeated name.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Number of distinct names.
    pub fn keys_len(&self) -> usize {
        self.keys().count()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    pub fn reserve(&mut self, additional: usize) {
        self.entries.reserve(additional);
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// The first value of `key`.
    pub fn get<K: AsHeaderName>(&self, key: K) -> Option<&T> {
        let run = self.find(&key)?;
        Some(&self.entries[run.start].1)
    }

    pub fn get_mut<K: AsHeaderName>(&mut self, key: K) -> Option<&mut T> {
        let run = self.find(&key)?;
        Some(&mut self.entries[run.start].1)
    }

    /// Every value of `key`, in order; empty if there is none.
    pub fn get_all<K: AsHeaderName>(&self, key: K) -> GetAll<'_, T> {
        let run = self.find(&key).unwrap_or(0..0);
        GetAll {
            run: &self.entries[run],
        }
    }

    pub fn contains_key<K: AsHeaderName>(&self, key: K) -> bool {
        self.find(&key).is_some()
    }

    /// Set `key` to `value` alone; returns the first value it replaced.
    pub fn insert<K: IntoHeaderName>(&mut self, key: K, value: T) -> Option<T> {
        let name = key.into_name();
        match self.find(&name) {
            Some(run) => {
                let old = mem::replace(&mut self.entries[run.start].1, value);
                self.entries.drain(run.start + 1..run.end);
                Some(old)
            }
            None => {
                self.entries.push((name, value));
                None
            }
        }
    }

    /// Like [`insert`](Self::insert), failing instead of growing past [`MAX_SIZE`].
    pub fn try_insert<K: IntoHeaderName>(
        &mut self,
        key: K,
        value: T,
    ) -> Result<Option<T>, MaxSizeReached> {
        let name = key.into_name();
        if self.entries.len() >= MAX_SIZE && !self.contains_key(&name) {
            return Err(MaxSizeReached(()));
        }
        Ok(self.insert(name, value))
    }

    /// Add `value` after the values `key` already has; returns whether it had any.
    pub fn append<K: IntoHeaderName>(&mut self, key: K, value: T) -> bool {
        let name = key.into_name();
        // Repeated names usually arrive one after another.
        if let Some((last, _)) = self.entries.last()
            && *last == name
        {
            self.entries.push((name, value));
            return true;
        }
        match self.find(&name) {
            Some(run) => {
                self.entries.insert(run.end, (name, value));
                true
            }
            None => {
                self.entries.push((name, value));
                false
            }
        }
    }

    /// Like [`append`](Self::append), failing instead of growing past [`MAX_SIZE`].
    pub fn try_append<K: IntoHeaderName>(
        &mut self,
        key: K,
        value: T,
    ) -> Result<bool, MaxSizeReached> {
        if self.entries.len() >= MAX_SIZE {
            return Err(MaxSizeReached(()));
        }
        Ok(self.append(key, value))
    }

    /// Remove every value of `key`; returns the first.
    pub fn remove<K: AsHeaderName>(&mut self, key: K) -> Option<T> {
        let run = self.find(&key)?;
        self.entries.drain(run).next().map(|(_, value)| value)
    }

    /// Every value with its name, names grouped.
    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            inner: self.entries.iter(),
        }
    }

    pub fn iter_mut(&mut self) -> IterMut<'_, T> {
        IterMut {
            inner: self.entries.iter_mut(),
        }
    }

    /// Each name once.
    pub fn keys(&self) -> Keys<'_, T> {
        Keys {
            entries: &self.entries,
        }
    }

    pub fn values(&self) -> impl Iterator<Item = &T> {
        self.entries.iter().map(|(_, value)| value)
    }

    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.entries.iter_mut().map(|(_, value)| value)
    }

    fn find<K: Lookup + ?Sized>(&self, key: &K) -> Option<Range<usize>> {
        let start = self
            .entries
            .iter()
            .position(|(name, _)| key.matches(name))?;
        let name = &self.entries[start].0;
        let len = self.entries[start..]
            .iter()
            .take_while(|(n, _)| n == name)
            .count();
        Some(start..start + len)
    }
}

impl<T> Default for HeaderMap<T> {
    fn default() -> HeaderMap<T> {
        HeaderMap::with_capacity(0)
    }
}

/// Same names with the same values in the same order per name; the order of names is free.
impl<T: PartialEq> PartialEq for HeaderMap<T> {
    fn eq(&self, other: &HeaderMap<T>) -> bool {
        self.len() == other.len()
            && self.keys().all(|name| {
                let theirs = other.get_all(name);
                self.get_all(name).iter().eq(theirs.iter())
            })
    }
}

impl<T: Eq> Eq for HeaderMap<T> {}

impl<T: fmt::Debug> fmt::Debug for HeaderMap<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

/// Panics if `key` has no value.
impl<K: AsHeaderName, T> Index<K> for HeaderMap<T> {
    type Output = T;

    fn index(&self, key: K) -> &T {
        self.get(key).expect("no entry found for key")
    }
}

impl<T> Extend<(HeaderName, T)> for HeaderMap<T> {
    fn extend<I: IntoIterator<Item = (HeaderName, T)>>(&mut self, iter: I) {
        for (name, value) in iter {
            self.append(name, value);
        }
    }
}

/// A `Some` name replaces that name's values; the `None` after it add to it, as
/// [`HeaderMap::into_iter`] yields them.
impl<T> Extend<(Option<HeaderName>, T)> for HeaderMap<T> {
    fn extend<I: IntoIterator<Item = (Option<HeaderName>, T)>>(&mut self, iter: I) {
        let mut current: Option<HeaderName> = None;
        for (name, value) in iter {
            match name {
                Some(name) => {
                    self.insert(&name, value);
                    current = Some(name);
                }
                None => {
                    let name = current.as_ref().expect("the first item must have a name");
                    self.append(name, value);
                }
            }
        }
    }
}

impl<T> FromIterator<(HeaderName, T)> for HeaderMap<T> {
    fn from_iter<I: IntoIterator<Item = (HeaderName, T)>>(iter: I) -> HeaderMap<T> {
        let mut map = HeaderMap::default();
        map.extend(iter);
        map
    }
}

pub struct Iter<'a, T> {
    inner: std::slice::Iter<'a, (HeaderName, T)>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = (&'a HeaderName, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(name, value)| (name, value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<T> ExactSizeIterator for Iter<'_, T> {}

pub struct IterMut<'a, T> {
    inner: std::slice::IterMut<'a, (HeaderName, T)>,
}

impl<'a, T> Iterator for IterMut<'a, T> {
    type Item = (&'a HeaderName, &'a mut T);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(name, value)| (&*name, value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

pub struct Keys<'a, T> {
    entries: &'a [(HeaderName, T)],
}

impl<'a, T> Iterator for Keys<'a, T> {
    type Item = &'a HeaderName;

    fn next(&mut self) -> Option<&'a HeaderName> {
        let (name, _) = self.entries.first()?;
        let run = self.entries.iter().take_while(|(n, _)| n == name).count();
        self.entries = &self.entries[run..];
        Some(name)
    }
}

/// Every value with its name; the name is given with the first value of each run only.
pub struct IntoIter<T> {
    inner: std::vec::IntoIter<(HeaderName, T)>,
    previous: Option<HeaderName>,
}

impl<T> Iterator for IntoIter<T> {
    type Item = (Option<HeaderName>, T);

    fn next(&mut self) -> Option<Self::Item> {
        let (name, value) = self.inner.next()?;
        if self.previous.as_ref() == Some(&name) {
            return Some((None, value));
        }
        self.previous = Some(name.clone());
        Some((Some(name), value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<T> IntoIterator for HeaderMap<T> {
    type Item = (Option<HeaderName>, T);
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> IntoIter<T> {
        IntoIter {
            inner: self.entries.into_iter(),
            previous: None,
        }
    }
}

impl<'a, T> IntoIterator for &'a HeaderMap<T> {
    type Item = (&'a HeaderName, &'a T);
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Iter<'a, T> {
        self.iter()
    }
}

impl<'a, T> IntoIterator for &'a mut HeaderMap<T> {
    type Item = (&'a HeaderName, &'a mut T);
    type IntoIter = IterMut<'a, T>;

    fn into_iter(self) -> IterMut<'a, T> {
        self.iter_mut()
    }
}

impl<'a, T> GetAll<'a, T> {
    pub fn iter(&self) -> ValueIter<'a, T> {
        ValueIter {
            inner: self.run.iter(),
        }
    }
}

impl<'a, T> IntoIterator for GetAll<'a, T> {
    type Item = &'a T;
    type IntoIter = ValueIter<'a, T>;

    fn into_iter(self) -> ValueIter<'a, T> {
        self.iter()
    }
}

impl<'a, T> IntoIterator for &GetAll<'a, T> {
    type Item = &'a T;
    type IntoIter = ValueIter<'a, T>;

    fn into_iter(self) -> ValueIter<'a, T> {
        self.iter()
    }
}

impl<T: PartialEq> PartialEq for GetAll<'_, T> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<T: fmt::Debug> fmt::Debug for GetAll<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

pub struct ValueIter<'a, T> {
    inner: std::slice::Iter<'a, (HeaderName, T)>,
}

impl<'a, T> Iterator for ValueIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<&'a T> {
        self.inner.next().map(|(_, value)| value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<'a, T> DoubleEndedIterator for ValueIter<'a, T> {
    fn next_back(&mut self) -> Option<&'a T> {
        self.inner.next_back().map(|(_, value)| value)
    }
}

impl<T> ExactSizeIterator for ValueIter<'_, T> {}

impl fmt::Display for MaxSizeReached {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("max header map size reached")
    }
}

impl std::error::Error for MaxSizeReached {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::{CONTENT_LENGTH, CONTENT_TYPE, SET_COOKIE};

    fn v(s: &'static str) -> HeaderValue {
        HeaderValue::from_static(s)
    }

    #[test]
    fn insert_replaces_every_value() {
        let mut map = HeaderMap::new();
        map.append(SET_COOKIE, v("a=1"));
        map.append(SET_COOKIE, v("b=2"));
        assert_eq!(map.insert(SET_COOKIE, v("c=3")), Some(v("a=1")));
        assert_eq!(
            map.get_all(SET_COOKIE).iter().collect::<Vec<_>>(),
            [&v("c=3")]
        );
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn appended_values_stay_with_their_name() {
        let mut map = HeaderMap::new();
        assert!(!map.append(SET_COOKIE, v("a=1")));
        map.insert(CONTENT_TYPE, v("text/plain"));
        assert!(map.append(&SET_COOKIE, v("b=2")));
        let order: Vec<_> = map
            .iter()
            .map(|(n, v)| (n.as_str(), v.to_str().unwrap()))
            .collect();
        assert_eq!(
            order,
            [
                ("set-cookie", "a=1"),
                ("set-cookie", "b=2"),
                ("content-type", "text/plain")
            ]
        );
        assert_eq!(map.keys().collect::<Vec<_>>(), [&SET_COOKIE, &CONTENT_TYPE]);
        assert_eq!(map.keys_len(), 2);
    }

    #[test]
    fn lookups_by_str_ignore_case() {
        let mut map = HeaderMap::new();
        map.insert("x-request-id", v("7"));
        assert_eq!(map.get("X-Request-Id"), Some(&v("7")));
        assert!(map.contains_key(String::from("x-request-id")));
        assert!(!map.contains_key("x request id"));
        assert_eq!(map["x-request-id"], "7");
    }

    #[test]
    fn remove_takes_the_whole_run() {
        let mut map = HeaderMap::new();
        map.append(SET_COOKIE, v("a"));
        map.append(SET_COOKIE, v("b"));
        map.insert(CONTENT_LENGTH, HeaderValue::from(3usize));
        assert_eq!(map.remove(SET_COOKIE), Some(v("a")));
        assert_eq!(map.len(), 1);
        assert!(map.get_all(SET_COOKIE).iter().next().is_none());
    }

    #[test]
    fn equality_ignores_name_order_not_value_order() {
        let a: HeaderMap = [
            (CONTENT_TYPE, v("x")),
            (SET_COOKIE, v("1")),
            (SET_COOKIE, v("2")),
        ]
        .into_iter()
        .collect();
        let b: HeaderMap = [
            (SET_COOKIE, v("1")),
            (SET_COOKIE, v("2")),
            (CONTENT_TYPE, v("x")),
        ]
        .into_iter()
        .collect();
        let c: HeaderMap = [
            (SET_COOKIE, v("2")),
            (SET_COOKIE, v("1")),
            (CONTENT_TYPE, v("x")),
        ]
        .into_iter()
        .collect();
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn into_iter_names_each_run_once_and_extend_rebuilds_it() {
        let mut map = HeaderMap::new();
        map.append(SET_COOKIE, v("a"));
        map.append(SET_COOKIE, v("b"));
        map.insert(CONTENT_TYPE, v("t"));
        let items: Vec<_> = map.clone().into_iter().collect();
        assert_eq!(items[0].0, Some(SET_COOKIE));
        assert_eq!(items[1].0, None);
        assert_eq!(items[2].0, Some(CONTENT_TYPE));
        let mut rebuilt = HeaderMap::new();
        rebuilt.extend(items);
        assert_eq!(rebuilt, map);
    }

    #[test]
    fn try_append_stops_at_max_size() {
        let mut map = HeaderMap::with_capacity(MAX_SIZE);
        for _ in 0..MAX_SIZE {
            map.append(SET_COOKIE, v("x"));
        }
        assert!(map.try_append(SET_COOKIE, v("x")).is_err());
        assert!(map.try_insert(SET_COOKIE, v("y")).is_ok());
    }

    #[test]
    fn debug_is_a_map() {
        let mut map = HeaderMap::new();
        map.insert(CONTENT_TYPE, v("text/html"));
        assert_eq!(format!("{map:?}"), r#"{"content-type": "text/html"}"#);
    }
}
