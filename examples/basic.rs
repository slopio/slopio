use bumpalo::{Bump, collections::Vec};

struct Foo<'arena> {
    arena: Bump,
    items: Vec<'arena, (&'arena str, &'arena str)>,
}

fn main() {}
