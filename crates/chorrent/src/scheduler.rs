//! Decides which piece to ask which peer for next.
//!
//! - The few pieces right after the first missing one are fetched in order
//!   (keeps the start of a file playable while it downloads).
//! - Everything else is rarest-first, so scarce pieces spread through the
//!   swarm before their only holder leaves.
//! - Endgame: once every missing piece is already requested, idle peers also
//!   request in-flight pieces, so one slow peer can't stall the finish.

use std::collections::HashMap;
use std::hash::Hash;

/// Maximum peers asked for the same piece at once during endgame.
const ENDGAME_DUPLICATES: usize = 3;
/// How many candidate pieces rarest-first looks at before choosing. Keeps a
/// pick cheap on huge shares. The scan starts at the first missing piece, so
/// ties go to the lowest index: writing files roughly front to back matters,
/// because on Windows a write far past the written part of a file stalls
/// while the filesystem zero-fills everything before it.
const RAREST_SAMPLE: usize = 64;

#[derive(Debug, Clone, PartialEq)]
enum Piece<K> {
    Missing,
    InFlight(Vec<K>),
    Done,
}

#[derive(Debug)]
pub(crate) struct Scheduler<K> {
    pieces: Vec<Piece<K>>,
    /// How many connected peers hold each piece.
    availability: Vec<u32>,
    peers: HashMap<K, Vec<bool>>,
    urgent_window: usize,
    /// Every piece before this one is Done.
    first_not_done: usize,
    missing: usize,
    done: usize,
}

impl<K: Eq + Hash + Clone> Scheduler<K> {
    /// `have` is what's already on disk (all false for a fresh download).
    pub fn new(have: &[bool], urgent_window: usize) -> Self {
        let pieces: Vec<_> = have.iter().map(|&h| if h { Piece::Done } else { Piece::Missing }).collect();
        let done = have.iter().filter(|&&h| h).count();
        let mut s = Self {
            availability: vec![0; pieces.len()],
            missing: pieces.len() - done,
            pieces,
            peers: HashMap::new(),
            urgent_window,
            first_not_done: 0,
            done,
        };
        s.advance_playhead();
        s
    }

    pub fn is_done(&self) -> bool {
        self.done == self.pieces.len()
    }

    pub fn add_peer(&mut self, peer: K, have: Vec<bool>) {
        self.remove_peer(&peer);
        for (i, &h) in have.iter().enumerate().take(self.pieces.len()) {
            if h {
                self.availability[i] += 1;
            }
        }
        self.peers.insert(peer, have);
    }

    pub fn set_peer_has(&mut self, peer: &K, piece: usize) {
        if let Some(bits) = self.peers.get_mut(peer)
            && piece < bits.len()
            && !bits[piece]
        {
            bits[piece] = true;
            self.availability[piece] += 1;
        }
    }

    /// Forget a peer; anything only it was fetching goes back to Missing.
    pub fn remove_peer(&mut self, peer: &K) {
        let Some(bits) = self.peers.remove(peer) else { return };
        for (i, h) in bits.into_iter().enumerate() {
            if h {
                self.availability[i] -= 1;
            }
        }
        for i in 0..self.pieces.len() {
            self.drop_request(i, peer);
        }
    }

    pub fn pick(&mut self, peer: &K) -> Option<usize> {
        let bits = self.peers.get(peer)?;
        let total = self.pieces.len();
        let wanted = |i: usize| bits[i] && self.pieces[i] == Piece::Missing;

        // 1. Urgent window, strictly in order.
        let window = self.first_not_done..(self.first_not_done + self.urgent_window).min(total);
        let mut choice = window.clone().find(|&i| wanted(i));

        // 2. Rarest-first over the next few candidates.
        if choice.is_none() && self.missing > 0 && total > 0 {
            let start = self.first_not_done;
            let mut best: Option<(usize, u32)> = None;
            let mut seen = 0;
            for i in (start..total).chain(0..start) {
                if !wanted(i) {
                    continue;
                }
                let rarity = self.availability[i];
                if best.is_none_or(|(_, r)| rarity < r) {
                    best = Some((i, rarity));
                }
                seen += 1;
                if seen >= RAREST_SAMPLE || rarity <= 1 {
                    break;
                }
            }
            choice = best.map(|(i, _)| i);
        }

        // 3. Endgame: help with pieces others are already fetching.
        if choice.is_none() && self.missing == 0 {
            choice = (self.first_not_done..total)
                .filter(|&i| bits[i])
                .filter_map(|i| match &self.pieces[i] {
                    Piece::InFlight(by) if by.len() < ENDGAME_DUPLICATES && !by.contains(peer) => Some((i, by.len())),
                    _ => None,
                })
                .min_by_key(|&(_, n)| n)
                .map(|(i, _)| i);
        }

        let i = choice?;
        match &mut self.pieces[i] {
            Piece::Missing => {
                self.pieces[i] = Piece::InFlight(vec![peer.clone()]);
                self.missing -= 1;
            }
            Piece::InFlight(by) => by.push(peer.clone()),
            Piece::Done => unreachable!("never picks a done piece"),
        }
        Some(i)
    }

    /// The piece is verified and on disk. Returns true the first time.
    pub fn complete(&mut self, piece: usize) -> bool {
        match std::mem::replace(&mut self.pieces[piece], Piece::Done) {
            Piece::Done => false,
            Piece::Missing => {
                self.missing -= 1;
                self.done += 1;
                self.advance_playhead();
                true
            }
            Piece::InFlight(_) => {
                self.done += 1;
                self.advance_playhead();
                true
            }
        }
    }

    /// A request failed. If the peer says it doesn't have the piece (or sent
    /// bad data), stop asking it for that piece.
    pub fn failed(&mut self, piece: usize, peer: &K, peer_lacks_it: bool) {
        self.drop_request(piece, peer);
        if peer_lacks_it
            && let Some(bits) = self.peers.get_mut(peer)
            && bits[piece]
        {
            bits[piece] = false;
            self.availability[piece] -= 1;
        }
    }

    fn drop_request(&mut self, piece: usize, peer: &K) {
        if let Piece::InFlight(by) = &mut self.pieces[piece] {
            by.retain(|p| p != peer);
            if by.is_empty() {
                self.pieces[piece] = Piece::Missing;
                self.missing += 1;
            }
        }
    }

    fn advance_playhead(&mut self) {
        while self.first_not_done < self.pieces.len() && self.pieces[self.first_not_done] == Piece::Done {
            self.first_not_done += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bits(pieces: &[usize], total: usize) -> Vec<bool> {
        let mut v = vec![false; total];
        for &p in pieces {
            v[p] = true;
        }
        v
    }

    #[test]
    fn urgent_window_goes_in_order_before_rarest() {
        let mut s = Scheduler::new(&bits(&[0, 1], 10), 3);
        s.add_peer("a", bits(&[2, 3, 9], 10));
        s.add_peer("b", bits(&[2, 3], 10));
        assert_eq!(s.pick(&"a"), Some(2));
        assert_eq!(s.pick(&"a"), Some(3));
        // Window (2..5) has nothing left for "a"; piece 9 is the rarest it has.
        assert_eq!(s.pick(&"a"), Some(9));
        assert_eq!(s.pick(&"b"), None);
    }

    #[test]
    fn rarest_piece_is_preferred_outside_the_window() {
        let mut s = Scheduler::new(&bits(&[], 6), 0);
        s.add_peer("a", bits(&[0, 1, 2, 3, 4, 5], 6));
        s.add_peer("b", bits(&[0, 1, 2, 3, 5], 6));
        assert_eq!(s.pick(&"a"), Some(4));
    }

    #[test]
    fn failures_and_departures_put_pieces_back() {
        let mut s = Scheduler::new(&bits(&[], 2), 2);
        s.add_peer("a", bits(&[0, 1], 2));
        s.add_peer("b", bits(&[0, 1], 2));
        assert_eq!(s.pick(&"a"), Some(0));
        s.failed(0, &"a", true); // a doesn't really have 0
        assert_eq!(s.pick(&"a"), Some(1));
        assert_eq!(s.pick(&"b"), Some(0));
        s.remove_peer(&"a"); // piece 1 is free again
        assert_eq!(s.pick(&"b"), Some(1));
    }

    #[test]
    fn endgame_duplicates_in_flight_pieces_and_finishes_once() {
        let mut s = Scheduler::new(&bits(&[0], 2), 2);
        s.add_peer("slow", bits(&[1], 2));
        s.add_peer("fast", bits(&[1], 2));
        assert_eq!(s.pick(&"slow"), Some(1));
        assert_eq!(s.pick(&"fast"), Some(1)); // endgame duplicate
        assert_eq!(s.pick(&"fast"), None); // already asked
        assert!(s.complete(1));
        assert!(!s.complete(1));
        assert!(s.is_done());
    }

    #[test]
    fn nothing_to_pick_from_unknown_or_empty_peers() {
        let mut s = Scheduler::new(&bits(&[], 3), 3);
        assert_eq!(s.pick(&"ghost"), None);
        s.add_peer("empty", bits(&[], 3));
        assert_eq!(s.pick(&"empty"), None);
    }
}
