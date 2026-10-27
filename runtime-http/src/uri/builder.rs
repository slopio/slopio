use super::{Authority, Parts, PathAndQuery, Scheme, Uri};
use crate::Error;

/// Builds a [`Uri`] part by part; the first invalid part is reported by `build`.
#[derive(Debug)]
pub struct Builder {
    parts: Result<Parts, Error>,
}

impl Builder {
    pub fn new() -> Builder {
        Builder {
            parts: Ok(Parts::default()),
        }
    }

    pub fn scheme<T>(self, scheme: T) -> Builder
    where
        Scheme: TryFrom<T>,
        <Scheme as TryFrom<T>>::Error: Into<Error>,
    {
        self.map(|mut parts| {
            parts.scheme = Some(Scheme::try_from(scheme).map_err(Into::into)?);
            Ok(parts)
        })
    }

    pub fn authority<T>(self, authority: T) -> Builder
    where
        Authority: TryFrom<T>,
        <Authority as TryFrom<T>>::Error: Into<Error>,
    {
        self.map(|mut parts| {
            parts.authority = Some(Authority::try_from(authority).map_err(Into::into)?);
            Ok(parts)
        })
    }

    pub fn path_and_query<T>(self, path_and_query: T) -> Builder
    where
        PathAndQuery: TryFrom<T>,
        <PathAndQuery as TryFrom<T>>::Error: Into<Error>,
    {
        self.map(|mut parts| {
            parts.path_and_query =
                Some(PathAndQuery::try_from(path_and_query).map_err(Into::into)?);
            Ok(parts)
        })
    }

    pub fn build(self) -> Result<Uri, Error> {
        Ok(Uri::from_parts(self.parts?)?)
    }

    fn map(self, f: impl FnOnce(Parts) -> Result<Parts, Error>) -> Builder {
        Builder {
            parts: self.parts.and_then(f),
        }
    }
}

impl Default for Builder {
    fn default() -> Builder {
        Builder::new()
    }
}

impl From<Uri> for Builder {
    fn from(uri: Uri) -> Builder {
        Builder {
            parts: Ok(uri.into_parts()),
        }
    }
}
