//! Cursor methods and slice-like traits shared by `Bytes` and `BytesMut`.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

use super::{Bytes, BytesMut};

/// `get_*` readers: each takes its bytes off the front and panics if fewer remain.
macro_rules! get_methods {
    () => {
        pub fn get_u8(&mut self) -> u8 {
            self.take_array::<1>()[0]
        }

        pub fn get_i8(&mut self) -> i8 {
            self.take_array::<1>()[0] as i8
        }

        $crate::bytes::impls::get_methods!(@int
            get_u16 get_u16_le u16,
            get_i16 get_i16_le i16,
            get_u32 get_u32_le u32,
            get_i32 get_i32_le i32,
            get_u64 get_u64_le u64,
            get_i64 get_i64_le i64,
            get_u128 get_u128_le u128,
            get_i128 get_i128_le i128,
            get_f32 get_f32_le f32,
            get_f64 get_f64_le f64,
        );

        /// A big-endian unsigned integer of `nbytes` bytes.
        ///
        /// Panics if `nbytes > 8`.
        pub fn get_uint(&mut self, nbytes: usize) -> u64 {
            assert!(nbytes <= 8, "get_uint of {nbytes} bytes");
            assert!(nbytes <= self.len(), "read of {nbytes} bytes past the end (len = {})", self.len());
            let mut array = [0; 8];
            array[8 - nbytes..].copy_from_slice(&self[..nbytes]);
            self.advance(nbytes);
            u64::from_be_bytes(array)
        }
    };
    (@int $($be:ident $le:ident $ty:ty,)*) => {
        $(
            #[doc = concat!("A big-endian `", stringify!($ty), "`.")]
            pub fn $be(&mut self) -> $ty {
                <$ty>::from_be_bytes(self.take_array())
            }

            #[doc = concat!("A little-endian `", stringify!($ty), "`.")]
            pub fn $le(&mut self) -> $ty {
                <$ty>::from_le_bytes(self.take_array())
            }
        )*
    };
}

/// `put_*` writers: each appends, growing the buffer as needed.
macro_rules! put_methods {
    () => {
        pub fn put_u8(&mut self, value: u8) {
            self.extend_from_slice(&[value]);
        }

        pub fn put_i8(&mut self, value: i8) {
            self.extend_from_slice(&[value as u8]);
        }

        $crate::bytes::impls::put_methods!(@int
            put_u16 put_u16_le u16,
            put_i16 put_i16_le i16,
            put_u32 put_u32_le u32,
            put_i32 put_i32_le i32,
            put_u64 put_u64_le u64,
            put_i64 put_i64_le i64,
            put_u128 put_u128_le u128,
            put_i128 put_i128_le i128,
            put_f32 put_f32_le f32,
            put_f64 put_f64_le f64,
        );
    };
    (@int $($be:ident $le:ident $ty:ty,)*) => {
        $(
            #[doc = concat!("Append a big-endian `", stringify!($ty), "`.")]
            pub fn $be(&mut self, value: $ty) {
                self.extend_from_slice(&value.to_be_bytes());
            }

            #[doc = concat!("Append a little-endian `", stringify!($ty), "`.")]
            pub fn $le(&mut self, value: $ty) {
                self.extend_from_slice(&value.to_le_bytes());
            }
        )*
    };
}

pub(crate) use get_methods;
pub(crate) use put_methods;

macro_rules! slice_like {
    ($ty:ty) => {
        impl AsRef<[u8]> for $ty {
            fn as_ref(&self) -> &[u8] {
                self
            }
        }

        impl Borrow<[u8]> for $ty {
            fn borrow(&self) -> &[u8] {
                self
            }
        }

        impl fmt::Debug for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "b\"{}\"", self.escape_ascii())
            }
        }

        impl Hash for $ty {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self[..].hash(state);
            }
        }

        impl PartialEq for $ty {
            fn eq(&self, other: &Self) -> bool {
                self[..] == other[..]
            }
        }

        impl Eq for $ty {}

        impl PartialOrd for $ty {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for $ty {
            fn cmp(&self, other: &Self) -> Ordering {
                self[..].cmp(&other[..])
            }
        }

        impl PartialEq<[u8]> for $ty {
            fn eq(&self, other: &[u8]) -> bool {
                self[..] == *other
            }
        }

        impl<const N: usize> PartialEq<[u8; N]> for $ty {
            fn eq(&self, other: &[u8; N]) -> bool {
                self[..] == other[..]
            }
        }

        impl PartialEq<str> for $ty {
            fn eq(&self, other: &str) -> bool {
                self[..] == *other.as_bytes()
            }
        }

        impl PartialEq<Vec<u8>> for $ty {
            fn eq(&self, other: &Vec<u8>) -> bool {
                self[..] == other[..]
            }
        }

        impl<T: ?Sized> PartialEq<&T> for $ty
        where
            $ty: PartialEq<T>,
        {
            fn eq(&self, other: &&T) -> bool {
                *self == **other
            }
        }

        impl PartialEq<$ty> for [u8] {
            fn eq(&self, other: &$ty) -> bool {
                *self == other[..]
            }
        }

        impl PartialEq<$ty> for str {
            fn eq(&self, other: &$ty) -> bool {
                *self.as_bytes() == other[..]
            }
        }

        impl PartialEq<$ty> for Vec<u8> {
            fn eq(&self, other: &$ty) -> bool {
                self[..] == other[..]
            }
        }
    };
}

slice_like!(Bytes);
slice_like!(BytesMut);

impl PartialEq<BytesMut> for Bytes {
    fn eq(&self, other: &BytesMut) -> bool {
        self[..] == other[..]
    }
}

impl PartialEq<Bytes> for BytesMut {
    fn eq(&self, other: &Bytes) -> bool {
        self[..] == other[..]
    }
}
