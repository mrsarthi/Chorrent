pub mod error;

pub mod stage1 {
    #[path = "chunker.rs"]
    pub mod chunker;
}

pub mod stage2 {
    #[path = "node.rs"]
    pub mod node;
}

pub mod stage3 {
    #[path = "protocol.rs"]
    pub mod protocol;
    #[path = "handler.rs"]
    pub mod handler;
}

pub mod stage4 {
    #[path = "scheduler.rs"]
    pub mod scheduler;
}

pub mod stage5 {
    #[path = "discovery.rs"]
    pub mod discovery;
}