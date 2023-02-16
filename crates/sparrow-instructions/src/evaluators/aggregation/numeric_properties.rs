/// Trait providing basic numeric properties for primitive types.
///
/// This allows defining the aggregation functions generically.
pub trait NumericProperties {
    const ZERO: Self;
    fn max(a: Self, b: Self) -> Self;
    fn min(a: Self, b: Self) -> Self;
    fn saturating_add(a: Self, b: Self) -> Self;
    fn as_f64(&self) -> f64;
}

impl NumericProperties for f32 {
    const ZERO: f32 = 0.0;

    fn max(a: Self, b: Self) -> Self {
        a.max(b)
    }
    fn min(a: Self, b: Self) -> Self {
        a.min(b)
    }
    fn as_f64(&self) -> f64 {
        *self as f64
    }

    fn saturating_add(a: Self, b: Self) -> Self {
        a + b
    }
}

impl NumericProperties for f64 {
    const ZERO: Self = 0.0;

    fn max(a: Self, b: Self) -> Self {
        a.max(b)
    }
    fn min(a: Self, b: Self) -> Self {
        a.min(b)
    }
    fn as_f64(&self) -> f64 {
        *self
    }
    fn saturating_add(a: Self, b: Self) -> Self {
        a + b
    }
}

impl NumericProperties for i32 {
    const ZERO: Self = 0;

    fn max(a: Self, b: Self) -> Self {
        a.max(b)
    }

    fn min(a: Self, b: Self) -> Self {
        a.min(b)
    }

    fn as_f64(&self) -> f64 {
        *self as f64
    }
    fn saturating_add(a: Self, b: Self) -> Self {
        a.saturating_add(b)
    }
}

impl NumericProperties for i64 {
    const ZERO: Self = 0;

    fn max(a: Self, b: Self) -> Self {
        a.max(b)
    }

    fn min(a: Self, b: Self) -> Self {
        a.min(b)
    }

    fn as_f64(&self) -> f64 {
        *self as f64
    }
    fn saturating_add(a: Self, b: Self) -> Self {
        a.saturating_add(b)
    }
}

impl NumericProperties for u32 {
    const ZERO: Self = 0;

    fn max(a: Self, b: Self) -> Self {
        a.max(b)
    }

    fn min(a: Self, b: Self) -> Self {
        a.min(b)
    }

    fn as_f64(&self) -> f64 {
        *self as f64
    }
    fn saturating_add(a: Self, b: Self) -> Self {
        a.saturating_add(b)
    }
}

impl NumericProperties for u64 {
    const ZERO: Self = 0;

    fn max(a: Self, b: Self) -> Self {
        a.max(b)
    }

    fn min(a: Self, b: Self) -> Self {
        a.min(b)
    }

    fn as_f64(&self) -> f64 {
        *self as f64
    }
    fn saturating_add(a: Self, b: Self) -> Self {
        a.saturating_add(b)
    }
}
