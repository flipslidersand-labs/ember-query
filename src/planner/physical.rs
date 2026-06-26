/// For now the physical plan is an alias of the logical plan.
/// Future work: add cost-based optimisations, join ordering, etc.
#[allow(unused_imports)]
pub use super::logical::LogicalPlan as PhysicalPlan;
