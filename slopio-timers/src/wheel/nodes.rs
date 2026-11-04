use std::task::Waker;

/// End of a list, or of the free list.
pub(super) const NIL: u32 = u32::MAX;
/// `prev` of a node on the free list.
const FREE: u32 = u32::MAX - 1;
/// `prev` of a node that fired and waits for its last poll.
const DONE: u32 = u32::MAX - 2;
/// `prev` with this bit set names the bucket the node heads, not a node: the
/// kernel's `pprev`, so an unlink never has to work out where the node sits.
pub(super) const BUCKET: u32 = 1 << 31;

/// One timer, 32 bytes: two per cache line.
pub(super) struct Node {
    pub(super) waker: Option<Waker>,
    pub(super) deadline: u64,
    /// Next node in the bucket, or next free node.
    pub(super) next: u32,
    /// Previous node, `BUCKET | bucket` when first, or `FREE` / `DONE`.
    pub(super) prev: u32,
}

const _: () = assert!(std::mem::size_of::<Node>() == 32);

pub(super) enum State {
    /// Linked in a bucket, waiting to fire.
    Waiting,
    /// Fired; freed by its last poll or its drop.
    Done,
    Free,
}

/// Every timer's node, addressed by a `u32` id that stays put: freed ids go on
/// a LIFO free list threaded through `next`, so reuse is cache-hot.
pub(super) struct Nodes {
    nodes: Vec<Node>,
    free: u32,
    live: usize,
}

impl Default for Nodes {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            free: NIL,
            live: 0,
        }
    }
}

impl Nodes {
    pub(super) fn alloc(&mut self, waker: &Waker, deadline: u64) -> u32 {
        self.live += 1;
        let node = Node {
            waker: Some(waker.clone()),
            deadline,
            next: NIL,
            prev: NIL,
        };
        if self.free != NIL {
            let id = self.free;
            self.free = self.nodes[id as usize].next;
            self.nodes[id as usize] = node;
            return id;
        }
        let id = self.nodes.len() as u32;
        assert!(id < BUCKET - 3, "timer ids exhausted");
        self.nodes.push(node);
        id
    }

    /// Back on the free list; the node must already be out of its bucket.
    pub(super) fn release(&mut self, id: u32) {
        self.live -= 1;
        let node = &mut self.nodes[id as usize];
        node.waker = None;
        node.prev = FREE;
        node.next = self.free;
        self.free = id;
    }

    /// Fired: the waker is taken, the node stays until its last poll.
    pub(super) fn fire(&mut self, id: u32) -> Option<Waker> {
        let node = &mut self.nodes[id as usize];
        node.prev = DONE;
        node.waker.take()
    }

    pub(super) fn state(&self, id: u32) -> State {
        match self.nodes[id as usize].prev {
            FREE => State::Free,
            DONE => State::Done,
            _ => State::Waiting,
        }
    }

    pub(super) fn live(&self) -> usize {
        self.live
    }

    #[inline]
    pub(super) fn get(&self, id: u32) -> &Node {
        &self.nodes[id as usize]
    }

    #[inline]
    pub(super) fn get_mut(&mut self, id: u32) -> &mut Node {
        &mut self.nodes[id as usize]
    }
}
