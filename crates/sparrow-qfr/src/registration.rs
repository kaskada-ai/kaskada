use crate::kaskada::sparrow::v1alpha::FlightRecord;

/// Trait for things that can be registered with the flight recorder.
pub trait ToRegistration {
    fn to_registration(&self) -> FlightRecord;
}

#[derive(Default)]
pub struct Registrations(Vec<FlightRecord>);

impl Registrations {
    pub fn add(&mut self, registration: impl ToRegistration) {
        self.0.push(registration.to_registration());
    }
}

pub struct Registration(once_cell::sync::Lazy<Registrations>);

impl Registration {
    pub const fn new(f: fn() -> Registrations) -> Self {
        Self(once_cell::sync::Lazy::new(f))
    }

    pub fn records(&self) -> &[FlightRecord] {
        &self.0 .0
    }
}

inventory::collect!(&'static Registration);
