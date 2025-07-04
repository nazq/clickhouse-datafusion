// TODO: Remove - when clickhouse-arrow is updated (and includes this enum)
//
// TODO: Docs - add additional docs linking to ClickHouse documentation
//
/// Non-exhaustive list of ClickHouse engines. [`Self::Other`] can always be used.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ClickHouseEngine {
    MergeTree,
    AggregatingMergeTree,
    CollapsingMergeTree,
    ReplacingMergeTree,
    SummingMergeTree,
    Memory,
    Log,
    StripeLog,
    TinyLog,
    Other(String),
}

impl<S> From<S> for ClickHouseEngine
where
    S: Into<String>,
{
    fn from(value: S) -> Self {
        let engine = value.into().to_uppercase();
        match engine.as_str() {
            "MERGE_TREE" => Self::MergeTree,
            "AGGREGATING_MERGE_TREE" => Self::AggregatingMergeTree,
            "COLLAPSING_MERGE_TREE" => Self::CollapsingMergeTree,
            "REPLACING_MERGE_TREE" => Self::ReplacingMergeTree,
            "SUMMING_MERGE_TREE" => Self::SummingMergeTree,
            "MEMORY" => Self::Memory,
            "LOG" => Self::Log,
            "STRIPE_LOG" => Self::StripeLog,
            "TINY_LOG" => Self::TinyLog,
            _ => Self::Other(engine),
        }
    }
}

impl std::fmt::Display for ClickHouseEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MergeTree => write!(f, "MergeTree"),
            Self::AggregatingMergeTree => write!(f, "AggregatingMergeTree"),
            Self::CollapsingMergeTree => write!(f, "CollapsingMergeTree"),
            Self::ReplacingMergeTree => write!(f, "ReplacingMergeTree"),
            Self::SummingMergeTree => write!(f, "SummingMergeTree"),
            Self::Memory => write!(f, "Memory"),
            Self::Log => write!(f, "Log"),
            Self::StripeLog => write!(f, "StripeLog"),
            Self::TinyLog => write!(f, "TinyLog"),
            Self::Other(engine) => write!(f, "{engine}"),
        }
    }
}
