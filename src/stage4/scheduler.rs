use std::collections::HashMap;
use rand::prelude::IndexedRandom;

/// For now, just a plain identifier string for a peer (their ticket, or
/// eventually their EndpointId). The scheduler doesn't need to know
/// anything about how peers are actually reached over the network.
pub type PeerId = String;

pub struct SwarmState {
    pub total_pieces: usize,
    /// Which pieces we already have.
    pub have: Vec<bool>,
    /// Which pieces each known peer has.
    pub peer_bitfields: HashMap<PeerId, Vec<bool>>,
}

impl SwarmState {
    /// If multiple peers have this piece, prefer whichever one holds
    /// fewer pieces overall — i.e. actually favor the scarcer peer,
    /// instead of picking arbitrarily.
    fn peer_with_piece(&self, piece: usize) -> Option<PeerId> {
        let min_count = self
            .peer_bitfields
            .iter()
            .filter(|(_, bits)| bits[piece])
            .map(|(_, bits)| bits.iter().filter(|&&b| b).count())
            .min()?;

        let candidates: Vec<&PeerId> = self
            .peer_bitfields
            .iter()
            .filter(|(_, bits)| bits[piece] && bits.iter().filter(|&&b| b).count() == min_count)
            .map(|(peer, _)| peer)
            .collect();

        candidates.choose(&mut rand::rng()).map(|&p| p.clone())
    }

    fn peers_with_piece_count(&self, piece: usize) -> usize {
        self.peer_bitfields.values().filter(|bits| bits[piece]).count()
    }
}

/// Decide the single next piece to request, and who to request it from.
/// `playhead` = the piece index playback is currently at.
/// `window`   = how many pieces ahead of the playhead count as "urgent."
pub fn next_piece_to_request(
    state: &SwarmState,
    playhead: usize,
    window: usize,
) -> Option<(usize, PeerId)> {
    // 1. Urgent zone: fill in order, earliest missing piece first.
    let window_end = (playhead + window).min(state.total_pieces);
    for piece in playhead..window_end {
        if !state.have[piece] {
            if let Some(peer) = state.peer_with_piece(piece) {
                return Some((piece, peer));
            }
        }
    }

    // 2. Everywhere else: rarest-first.
    let mut best: Option<(usize, usize)> = None; // (piece, how many peers have it)
    for piece in 0..state.total_pieces {
        if state.have[piece] {
            continue;
        }
        let rarity = state.peers_with_piece_count(piece);
        if rarity == 0 {
            continue; // nobody has it yet, can't request it
        }
        if best.is_none_or(|(_, best_rarity)| rarity < best_rarity) {
            best = Some((piece, rarity));
        }
    }

    let (piece, _) = best?;
    state.peer_with_piece(piece).map(|peer| (piece, peer))
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
    fn urgent_window_picks_earliest_missing_in_order_even_if_not_rarest() {
        let mut peers = HashMap::new();
        peers.insert("fast".to_string(), bits(&[5], 10));
        peers.insert("slow".to_string(), bits(&[2], 10));

        let state = SwarmState { total_pieces: 10, have: bits(&[], 10), peer_bitfields: peers };

        let result = next_piece_to_request(&state, 2, 3);
        assert_eq!(result, Some((2, "slow".to_string())));
    }

    #[test]
    fn falls_back_to_rarest_first_beyond_the_window() {
        let mut peers = HashMap::new();
        peers.insert("a".to_string(), bits(&[0, 1, 2], 5));
        peers.insert("b".to_string(), bits(&[0, 1], 5));

        let state = SwarmState { total_pieces: 5, have: bits(&[], 5), peer_bitfields: peers };

        let result = next_piece_to_request(&state, 0, 0);
        assert_eq!(result, Some((2, "a".to_string())));
    }

    #[test]
    fn returns_none_when_nothing_is_available() {
        let state = SwarmState { total_pieces: 3, have: bits(&[0, 1, 2], 3), peer_bitfields: HashMap::new() };
        assert_eq!(next_piece_to_request(&state, 0, 3), None);
    }
}