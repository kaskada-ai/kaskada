use anyhow::Context;
use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

use crate::min_heap::{HasPriority, MinHeap};

pub(super) trait TickProducer: Send + Sync {
    /// Returns true if the given date time is a tick for this producer.
    fn is_tick(&self, time: NaiveDateTime) -> bool;

    /// Truncate (round down) to a time produced by this producer.
    ///
    /// This is *inclusive* of the time -- if it is a valid tick it will be
    /// returned.
    fn truncate(&self, time: NaiveDateTime) -> anyhow::Result<NaiveDateTime>;

    /// Return the next tick after the given time.
    ///
    /// This is exclusive -- if the `time` is a tick this returns the tick after
    /// it.
    ///
    /// Returns `None` if `NaiveDateTime` cannot represent the next tick.
    fn next(&self, time: NaiveDateTime) -> anyhow::Result<NaiveDateTime> {
        self.next_tick(self.truncate(time)?)
    }

    /// Return the next tick at or after the given time.
    ///
    /// Tihs is inclusive -- if the `time` is a tick this reutrns it.
    fn next_inclusive(&self, time: NaiveDateTime) -> anyhow::Result<NaiveDateTime> {
        if self.is_tick(time) {
            Ok(time)
        } else {
            self.next_tick(self.truncate(time)?)
        }
    }

    /// Return the next tick after the given tick.
    ///
    /// This will panic in debug builds if called on a non-tick. Use
    /// `next(time)` in that case.
    fn next_tick(&self, tick: NaiveDateTime) -> anyhow::Result<NaiveDateTime>;
}

#[derive(Debug, Clone)]
pub(super) struct MinutelyTickProducer;

fn zero_time() -> NaiveTime {
    NaiveTime::default()
}

impl TickProducer for MinutelyTickProducer {
    fn is_tick(&self, time: NaiveDateTime) -> bool {
        time.second() == 0 && time.nanosecond() == 0
    }

    fn truncate(&self, time: NaiveDateTime) -> anyhow::Result<NaiveDateTime> {
        if self.is_tick(time) {
            Ok(time)
        } else {
            let date = time.date();
            let time = NaiveTime::from_hms_opt(time.hour(), time.minute(), 0)
                .context("unable to represent time")?;
            Ok(NaiveDateTime::new(date, time))
        }
    }

    fn next_tick(&self, tick: NaiveDateTime) -> anyhow::Result<NaiveDateTime> {
        debug_assert!(
            self.is_tick(tick),
            "Expected time to be aligned to {self:?}, but was: {tick:?}"
        );

        if tick.hour() == 23 && tick.minute() == 59 {
            let next_date = tick
                .date()
                .succ_opt()
                .context("next date not representable")?;
            Ok(NaiveDateTime::new(next_date, zero_time()))
        } else if tick.minute() == 59 {
            tick.with_minute(0)
                .context("zero minute not representable")?
                .with_hour(tick.hour() + 1)
                .context("next hour not representable")
        } else {
            tick.with_minute(tick.minute() + 1)
                .context("next minute not representable")
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct HourlyTickProducer;

impl TickProducer for HourlyTickProducer {
    fn is_tick(&self, time: NaiveDateTime) -> bool {
        time.minute() == 0 && time.second() == 0 && time.nanosecond() == 0
    }

    fn truncate(&self, time: NaiveDateTime) -> anyhow::Result<NaiveDateTime> {
        if self.is_tick(time) {
            Ok(time)
        } else {
            let date = time.date();
            let time =
                NaiveTime::from_hms_opt(time.hour(), 0, 0).context("hour not representable")?;
            Ok(NaiveDateTime::new(date, time))
        }
    }

    fn next_tick(&self, tick: NaiveDateTime) -> anyhow::Result<NaiveDateTime> {
        debug_assert!(
            self.is_tick(tick),
            "Expected time to be aligned to {self:?}, but was: {tick:?}"
        );

        if tick.hour() == 23 {
            let date = tick
                .date()
                .succ_opt()
                .context("next date not representable")?;
            Ok(NaiveDateTime::new(date, zero_time()))
        } else {
            tick.with_hour(tick.hour() + 1)
                .context("next hour not representable")
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct DailyTickProducer;

impl TickProducer for DailyTickProducer {
    fn is_tick(&self, time: NaiveDateTime) -> bool {
        time.hour() == 0 && time.minute() == 0 && time.second() == 0
    }

    fn truncate(&self, time: NaiveDateTime) -> anyhow::Result<NaiveDateTime> {
        if self.is_tick(time) {
            Ok(time)
        } else {
            Ok(NaiveDateTime::new(time.date(), zero_time()))
        }
    }

    fn next_tick(&self, tick: NaiveDateTime) -> anyhow::Result<NaiveDateTime> {
        debug_assert!(
            self.is_tick(tick),
            "Expected time to be aligned to {self:?}, but was: {tick:?}"
        );

        let date = tick
            .date()
            .succ_opt()
            .context("next date not representable")?;
        Ok(NaiveDateTime::new(date, zero_time()))
    }
}

#[derive(Debug, Clone)]
pub(super) struct MonthlyTickProducer;

impl TickProducer for MonthlyTickProducer {
    fn is_tick(&self, time: NaiveDateTime) -> bool {
        time.day() == 1
            && time.hour() == 0
            && time.minute() == 0
            && time.second() == 0
            && time.nanosecond() == 0
    }

    fn truncate(&self, time: NaiveDateTime) -> anyhow::Result<NaiveDateTime> {
        if self.is_tick(time) {
            Ok(time)
        } else {
            let date = NaiveDate::from_ymd_opt(time.year(), time.month(), 1)
                .context("date not representable")?;
            Ok(NaiveDateTime::new(date, zero_time()))
        }
    }

    fn next_tick(&self, tick: NaiveDateTime) -> anyhow::Result<NaiveDateTime> {
        debug_assert!(
            self.is_tick(tick),
            "Expected time to be aligned to {self:?}, but was: {tick:?}"
        );

        if tick.month() == 12 {
            tick.with_month(1)
                .context("month 1")?
                .with_year(tick.year() + 1)
                .context("next year not representable")
        } else {
            tick.with_day(1)
                .context("day 1")?
                .with_month(tick.month() + 1)
                .context("next month not representable")
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct YearlyTickProducer;

impl TickProducer for YearlyTickProducer {
    fn is_tick(&self, time: NaiveDateTime) -> bool {
        time.month() == 1
            && time.day() == 1
            && time.hour() == 0
            && time.minute() == 0
            && time.second() == 0
            && time.nanosecond() == 0
    }

    fn truncate(&self, time: NaiveDateTime) -> anyhow::Result<NaiveDateTime> {
        if self.is_tick(time) {
            Ok(time)
        } else {
            let date = NaiveDate::from_ymd_opt(time.year(), 1, 1)
                .context("start of year not representable")?;
            Ok(NaiveDateTime::new(date, zero_time()))
        }
    }

    fn next_tick(&self, tick: NaiveDateTime) -> anyhow::Result<NaiveDateTime> {
        debug_assert!(
            self.is_tick(tick),
            "Expected time to be aligned to {self:?}, but was: {tick:?}"
        );

        tick.with_year(tick.year() + 1)
            .context("next year not representable")
    }
}

/// An iterator over the merged times produced by one or more tick producers.
pub(super) struct TickIter {
    producers: Vec<Box<dyn TickProducer>>,
    pending: MinHeap<(NaiveDateTime, usize)>,
    /// Generate ticks less than this time
    end_exclusive: NaiveDateTime,
}

impl std::fmt::Debug for TickIter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TickIter")
            .field("producers", &format!("{} entries", self.producers.len()))
            .field("pending", &format!("{} entries", self.pending.len()))
            .field("end_exclusive", &self.end_exclusive)
            .finish()
    }
}

impl HasPriority for (NaiveDateTime, usize) {
    type Priority = NaiveDateTime;

    fn priority(&self) -> Self::Priority {
        self.0
    }
}

impl TickIter {
    pub(super) fn try_new(
        producers: Vec<Box<dyn TickProducer>>,
        start_inclusive: NaiveDateTime,
        end_exclusive: NaiveDateTime,
    ) -> anyhow::Result<Self> {
        let mut pending = Vec::with_capacity(producers.len());
        for (index, producer) in producers.iter().enumerate() {
            let first_tick = producer.next_inclusive(start_inclusive)?;
            if first_tick < end_exclusive {
                pending.push((first_tick, index))
            }
        }
        let pending = MinHeap::from(pending);

        Ok(Self {
            producers,
            pending,
            end_exclusive,
        })
    }

    fn requeue(&mut self, time: NaiveDateTime, index: usize) {
        let next_tick = self.producers[index]
            .next_tick(time)
            .expect("unable to create next tick");
        if next_tick < self.end_exclusive {
            self.pending.push((next_tick, index));
        }
    }
}

impl Iterator for TickIter {
    type Item = NaiveDateTime;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((next_time, next_index)) = self.pending.pop() {
            if next_time >= self.end_exclusive {
                None
            } else {
                // Queue the next item from this producer.
                self.requeue(next_time, next_index);

                // Queue next items from all producers that had the same time.
                while let Some((queued_time, _)) = self.pending.peek() {
                    if queued_time > &next_time {
                        break;
                    }

                    let (queued_time, queued_index) = self.pending.pop().expect("peek was Some");
                    self.requeue(queued_time, queued_index);
                }

                // Return the tick time
                Some(next_time)
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::{DailyTickProducer, HourlyTickProducer, TickIter};

    mod minutely {
        use chrono::NaiveDate;

        use super::super::{MinutelyTickProducer, TickProducer};

        #[test]
        fn test_is_tick() {
            let producer = MinutelyTickProducer;

            let date = NaiveDate::from_ymd_opt(2020, 3, 14).unwrap();
            assert!(producer.is_tick(date.and_hms_opt(2, 0, 0).unwrap()));
            assert!(producer.is_tick(date.and_hms_opt(23, 59, 0).unwrap()));
            assert!(producer.is_tick(date.and_hms_opt(0, 22, 0).unwrap()));
            assert!(!producer.is_tick(date.and_hms_opt(0, 1, 1).unwrap()));
        }

        #[test]
        fn test_truncate() {
            let producer = MinutelyTickProducer;

            let date = NaiveDate::from_ymd_opt(2020, 3, 14).unwrap();
            assert_eq!(
                date.and_hms_opt(2, 6, 0).unwrap(),
                producer
                    .truncate(date.and_hms_opt(2, 6, 33).unwrap())
                    .unwrap()
            );
            assert_eq!(
                date.and_hms_opt(2, 0, 0).unwrap(),
                producer
                    .truncate(date.and_hms_opt(2, 0, 3).unwrap())
                    .unwrap()
            );
        }

        #[test]
        fn test_next() {
            let producer = MinutelyTickProducer;

            let date = NaiveDate::from_ymd_opt(2020, 3, 14).unwrap();
            assert_eq!(
                date.and_hms_opt(2, 1, 0).unwrap(),
                producer.next(date.and_hms_opt(2, 0, 0).unwrap()).unwrap()
            );
            assert_eq!(
                date.and_hms_opt(1, 1, 0).unwrap(),
                producer.next(date.and_hms_opt(1, 0, 10).unwrap()).unwrap()
            );
            assert_eq!(
                date.and_hms_opt(2, 0, 0).unwrap(),
                producer.next(date.and_hms_opt(1, 59, 1).unwrap()).unwrap()
            );
            assert_eq!(
                date.succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap(),
                producer.next(date.and_hms_opt(23, 59, 1).unwrap()).unwrap()
            );
        }

        #[test]
        fn test_next_tick() {
            let producer = MinutelyTickProducer;

            let date = NaiveDate::from_ymd_opt(2020, 3, 14).unwrap();
            assert_eq!(
                date.and_hms_opt(2, 1, 0).unwrap(),
                producer
                    .next_tick(date.and_hms_opt(2, 0, 0).unwrap())
                    .unwrap()
            );
            assert_eq!(
                date.and_hms_opt(2, 34, 0).unwrap(),
                producer
                    .next_tick(date.and_hms_opt(2, 33, 0).unwrap())
                    .unwrap()
            );
        }
    }

    mod hourly {
        use chrono::NaiveDate;

        use super::super::{HourlyTickProducer, TickProducer};

        #[test]
        fn test_is_tick() {
            let producer = HourlyTickProducer;

            let date = NaiveDate::from_ymd_opt(2020, 3, 14).unwrap();
            assert!(producer.is_tick(date.and_hms_opt(2, 0, 0).unwrap()));
            assert!(producer.is_tick(date.and_hms_opt(23, 0, 0).unwrap()));
            assert!(producer.is_tick(date.and_hms_opt(0, 0, 0).unwrap()));
            assert!(!producer.is_tick(date.and_hms_opt(0, 1, 0).unwrap()));
        }

        #[test]
        fn test_truncate() {
            let producer = HourlyTickProducer;

            let date = NaiveDate::from_ymd_opt(2020, 3, 14).unwrap();
            assert_eq!(
                date.and_hms_opt(2, 0, 0).unwrap(),
                producer
                    .truncate(date.and_hms_opt(2, 0, 0).unwrap())
                    .unwrap()
            );
            assert_eq!(
                date.and_hms_opt(2, 0, 0).unwrap(),
                producer
                    .truncate(date.and_hms_opt(2, 10, 3).unwrap())
                    .unwrap()
            );
        }

        #[test]
        fn test_next() {
            let producer = HourlyTickProducer;

            let date = NaiveDate::from_ymd_opt(2020, 3, 14).unwrap();
            assert_eq!(
                date.and_hms_opt(3, 0, 0).unwrap(),
                producer.next(date.and_hms_opt(2, 0, 0).unwrap()).unwrap()
            );
            assert_eq!(
                date.and_hms_opt(3, 0, 0).unwrap(),
                producer.next(date.and_hms_opt(2, 10, 3).unwrap()).unwrap()
            );
            assert_eq!(
                date.succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap(),
                producer.next(date.and_hms_opt(23, 10, 3).unwrap()).unwrap()
            );
        }

        #[test]
        fn test_next_tick() {
            let producer = HourlyTickProducer;

            let date = NaiveDate::from_ymd_opt(2020, 3, 14).unwrap();
            assert_eq!(
                date.and_hms_opt(3, 0, 0).unwrap(),
                producer
                    .next_tick(date.and_hms_opt(2, 0, 0).unwrap())
                    .unwrap()
            );
            assert_eq!(
                date.and_hms_opt(3, 0, 0).unwrap(),
                producer
                    .next_tick(date.and_hms_opt(2, 0, 0).unwrap())
                    .unwrap()
            );
            assert_eq!(
                date.succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap(),
                producer
                    .next_tick(date.and_hms_opt(23, 0, 0).unwrap())
                    .unwrap()
            );
        }
    }

    mod daily {
        use chrono::NaiveDate;

        use super::super::{DailyTickProducer, TickProducer};

        #[test]
        fn test_is_tick() {
            let producer = DailyTickProducer;

            let date = NaiveDate::from_ymd_opt(2020, 3, 14).unwrap();
            assert!(!producer.is_tick(date.and_hms_opt(2, 0, 0).unwrap()));
            assert!(!producer.is_tick(date.and_hms_opt(23, 0, 0).unwrap()));
            assert!(producer.is_tick(date.and_hms_opt(0, 0, 0).unwrap()));
            assert!(!producer.is_tick(date.and_hms_opt(0, 1, 0).unwrap()));
        }

        #[test]
        fn test_truncate() {
            let producer = DailyTickProducer;

            let date = NaiveDate::from_ymd_opt(2020, 3, 14).unwrap();
            assert_eq!(
                date.and_hms_opt(0, 0, 0).unwrap(),
                producer
                    .truncate(date.and_hms_opt(2, 0, 0).unwrap())
                    .unwrap()
            );
            assert_eq!(
                date.and_hms_opt(0, 0, 0).unwrap(),
                producer
                    .truncate(date.and_hms_opt(23, 10, 3).unwrap())
                    .unwrap()
            );
            assert_eq!(
                date.succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap(),
                producer
                    .truncate(date.succ_opt().unwrap().and_hms_opt(23, 10, 3).unwrap())
                    .unwrap()
            );
        }

        #[test]
        fn test_next() {
            let producer = DailyTickProducer;

            let date = NaiveDate::from_ymd_opt(2020, 3, 14).unwrap();
            assert_eq!(
                date.succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap(),
                producer.next(date.and_hms_opt(2, 0, 0).unwrap()).unwrap()
            );
            assert_eq!(
                date.succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap(),
                producer.next(date.and_hms_opt(2, 10, 3).unwrap()).unwrap()
            );
        }

        #[test]
        fn test_next_tick() {
            let producer = DailyTickProducer;

            let date = NaiveDate::from_ymd_opt(2020, 3, 14).unwrap();
            assert_eq!(
                date.succ_opt().unwrap().and_hms_opt(0, 0, 0).unwrap(),
                producer
                    .next_tick(date.and_hms_opt(0, 0, 0).unwrap())
                    .unwrap()
            );
        }
    }

    mod monthly {
        use chrono::NaiveDate;

        use super::super::{MonthlyTickProducer, TickProducer};

        #[test]
        fn test_is_tick() {
            let producer = MonthlyTickProducer;

            assert!(producer.is_tick(
                NaiveDate::from_ymd_opt(2020, 3, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
            ));
            assert!(!producer.is_tick(
                NaiveDate::from_ymd_opt(2020, 3, 14)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
            ));
            assert!(!producer.is_tick(
                NaiveDate::from_ymd_opt(2020, 3, 1)
                    .unwrap()
                    .and_hms_opt(1, 0, 0)
                    .unwrap()
            ));
            assert!(!producer.is_tick(
                NaiveDate::from_ymd_opt(2020, 3, 1)
                    .unwrap()
                    .and_hms_opt(1, 0, 0)
                    .unwrap()
            ));
            assert!(!producer.is_tick(
                NaiveDate::from_ymd_opt(2020, 3, 1)
                    .unwrap()
                    .and_hms_opt(0, 1, 0)
                    .unwrap()
            ));
            assert!(!producer.is_tick(
                NaiveDate::from_ymd_opt(2020, 3, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 1)
                    .unwrap()
            ));
            assert!(producer.is_tick(
                NaiveDate::from_ymd_opt(2020, 4, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
            ));
        }

        #[test]
        fn test_truncate() {
            let producer = MonthlyTickProducer;

            assert_eq!(
                NaiveDate::from_ymd_opt(2020, 3, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                producer
                    .truncate(
                        NaiveDate::from_ymd_opt(2020, 3, 14)
                            .unwrap()
                            .and_hms_opt(2, 0, 0)
                            .unwrap()
                    )
                    .unwrap()
            );
            assert_eq!(
                NaiveDate::from_ymd_opt(2020, 3, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                producer
                    .truncate(
                        NaiveDate::from_ymd_opt(2020, 3, 1)
                            .unwrap()
                            .and_hms_opt(2, 0, 0)
                            .unwrap()
                    )
                    .unwrap()
            );
            assert_eq!(
                NaiveDate::from_ymd_opt(2020, 4, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                producer
                    .truncate(
                        NaiveDate::from_ymd_opt(2020, 4, 3)
                            .unwrap()
                            .and_hms_opt(0, 1, 0)
                            .unwrap()
                    )
                    .unwrap()
            );
        }

        #[test]
        fn test_next() {
            let producer = MonthlyTickProducer;

            assert_eq!(
                NaiveDate::from_ymd_opt(2020, 4, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                producer
                    .next(
                        NaiveDate::from_ymd_opt(2020, 3, 14)
                            .unwrap()
                            .and_hms_opt(2, 0, 0)
                            .unwrap()
                    )
                    .unwrap()
            );
            assert_eq!(
                NaiveDate::from_ymd_opt(2021, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                producer
                    .next(
                        NaiveDate::from_ymd_opt(2020, 12, 25)
                            .unwrap()
                            .and_hms_opt(2, 0, 0)
                            .unwrap()
                    )
                    .unwrap()
            );
        }

        #[test]
        fn test_next_tick() {
            let producer = MonthlyTickProducer;

            assert_eq!(
                NaiveDate::from_ymd_opt(2020, 4, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                producer
                    .next(
                        NaiveDate::from_ymd_opt(2020, 3, 1)
                            .unwrap()
                            .and_hms_opt(0, 0, 0)
                            .unwrap()
                    )
                    .unwrap()
            );
            assert_eq!(
                NaiveDate::from_ymd_opt(2021, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                producer
                    .next(
                        NaiveDate::from_ymd_opt(2020, 12, 1)
                            .unwrap()
                            .and_hms_opt(0, 0, 0)
                            .unwrap()
                    )
                    .unwrap()
            );
        }
    }

    mod yearly {
        use chrono::NaiveDate;

        use super::super::{TickProducer, YearlyTickProducer};

        #[test]
        fn test_is_tick() {
            let producer = YearlyTickProducer;

            assert!(producer.is_tick(
                NaiveDate::from_ymd_opt(2020, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
            ));
            assert!(producer.is_tick(
                NaiveDate::from_ymd_opt(2021, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
            ));
            assert!(!producer.is_tick(
                NaiveDate::from_ymd_opt(2020, 3, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
            ));
            assert!(!producer.is_tick(
                NaiveDate::from_ymd_opt(2020, 3, 14)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
            ));
            assert!(!producer.is_tick(
                NaiveDate::from_ymd_opt(2020, 3, 1)
                    .unwrap()
                    .and_hms_opt(1, 0, 0)
                    .unwrap()
            ));
            assert!(!producer.is_tick(
                NaiveDate::from_ymd_opt(2020, 3, 1)
                    .unwrap()
                    .and_hms_opt(1, 0, 0)
                    .unwrap()
            ));
            assert!(!producer.is_tick(
                NaiveDate::from_ymd_opt(2020, 3, 1)
                    .unwrap()
                    .and_hms_opt(0, 1, 0)
                    .unwrap()
            ));
            assert!(!producer.is_tick(
                NaiveDate::from_ymd_opt(2020, 3, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 1)
                    .unwrap()
            ));
        }

        #[test]
        fn test_truncate() {
            let producer = YearlyTickProducer;

            assert_eq!(
                NaiveDate::from_ymd_opt(2020, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                producer
                    .truncate(
                        NaiveDate::from_ymd_opt(2020, 3, 14)
                            .unwrap()
                            .and_hms_opt(2, 0, 0)
                            .unwrap()
                    )
                    .unwrap()
            );
            assert_eq!(
                NaiveDate::from_ymd_opt(2021, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                producer
                    .truncate(
                        NaiveDate::from_ymd_opt(2021, 3, 1)
                            .unwrap()
                            .and_hms_opt(2, 0, 0)
                            .unwrap()
                    )
                    .unwrap()
            );
            assert_eq!(
                NaiveDate::from_ymd_opt(2020, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                producer
                    .truncate(
                        NaiveDate::from_ymd_opt(2020, 4, 3)
                            .unwrap()
                            .and_hms_opt(0, 1, 0)
                            .unwrap()
                    )
                    .unwrap()
            );
        }

        #[test]
        fn test_next() {
            let producer = YearlyTickProducer;

            assert_eq!(
                NaiveDate::from_ymd_opt(2021, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                producer
                    .next(
                        NaiveDate::from_ymd_opt(2020, 3, 14)
                            .unwrap()
                            .and_hms_opt(2, 0, 0)
                            .unwrap()
                    )
                    .unwrap()
            );
            assert_eq!(
                NaiveDate::from_ymd_opt(2021, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                producer
                    .next(
                        NaiveDate::from_ymd_opt(2020, 12, 25)
                            .unwrap()
                            .and_hms_opt(2, 0, 0)
                            .unwrap()
                    )
                    .unwrap()
            );
        }

        #[test]
        fn test_next_tick() {
            let producer = YearlyTickProducer;

            assert_eq!(
                NaiveDate::from_ymd_opt(2021, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                producer
                    .next(
                        NaiveDate::from_ymd_opt(2020, 1, 1)
                            .unwrap()
                            .and_hms_opt(0, 0, 0)
                            .unwrap()
                    )
                    .unwrap()
            );
        }
    }

    #[test]
    fn test_tick_iter_daily() {
        let times: Vec<_> = TickIter::try_new(
            vec![Box::new(DailyTickProducer)],
            NaiveDate::from_ymd_opt(2019, 2, 24)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2019, 3, 3)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        )
        .unwrap()
        .collect();

        assert_eq!(
            times,
            vec![
                NaiveDate::from_ymd_opt(2019, 2, 24)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2019, 2, 25)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2019, 2, 26)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2019, 2, 27)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2019, 2, 28)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2019, 3, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2019, 3, 2)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ]
        )
    }

    #[test]
    fn test_tick_iter_daily_leap_year() {
        let times: Vec<_> = TickIter::try_new(
            vec![Box::new(DailyTickProducer)],
            NaiveDate::from_ymd_opt(2020, 2, 24)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2020, 3, 3)
                .unwrap()
                .and_hms_opt(0, 1, 0)
                .unwrap(),
        )
        .unwrap()
        .collect();

        assert_eq!(
            times,
            vec![
                NaiveDate::from_ymd_opt(2020, 2, 24)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 25)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 26)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 27)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 28)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 29)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 3, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 3, 2)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 3, 3)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            ]
        )
    }

    #[test]
    fn test_tick_iter_daily_and_hourly() {
        let times: Vec<_> = TickIter::try_new(
            vec![Box::new(DailyTickProducer), Box::new(HourlyTickProducer)],
            NaiveDate::from_ymd_opt(2020, 2, 28)
                .unwrap()
                .and_hms_opt(8, 5, 0)
                .unwrap(),
            NaiveDate::from_ymd_opt(2020, 2, 29)
                .unwrap()
                .and_hms_opt(2, 1, 0)
                .unwrap(),
        )
        .unwrap()
        .collect();

        assert_eq!(
            times,
            vec![
                NaiveDate::from_ymd_opt(2020, 2, 28)
                    .unwrap()
                    .and_hms_opt(9, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 28)
                    .unwrap()
                    .and_hms_opt(10, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 28)
                    .unwrap()
                    .and_hms_opt(11, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 28)
                    .unwrap()
                    .and_hms_opt(12, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 28)
                    .unwrap()
                    .and_hms_opt(13, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 28)
                    .unwrap()
                    .and_hms_opt(14, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 28)
                    .unwrap()
                    .and_hms_opt(15, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 28)
                    .unwrap()
                    .and_hms_opt(16, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 28)
                    .unwrap()
                    .and_hms_opt(17, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 28)
                    .unwrap()
                    .and_hms_opt(18, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 28)
                    .unwrap()
                    .and_hms_opt(19, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 28)
                    .unwrap()
                    .and_hms_opt(20, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 28)
                    .unwrap()
                    .and_hms_opt(21, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 28)
                    .unwrap()
                    .and_hms_opt(22, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 28)
                    .unwrap()
                    .and_hms_opt(23, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 29)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 29)
                    .unwrap()
                    .and_hms_opt(1, 0, 0)
                    .unwrap(),
                NaiveDate::from_ymd_opt(2020, 2, 29)
                    .unwrap()
                    .and_hms_opt(2, 0, 0)
                    .unwrap(),
            ]
        )
    }

    // TODO: Proptest for tick iteration. We could easily verify a variety of
    // properties:
    // - Choose two intervals, ensure that all results are from one or the
    //   other.
    // - Choose one or more intervals, ensure that no time is generated twice.
    // - Choose one or more intervals, ensure all values are in expected range.
    // - Choose one or more intervals, and ensure that all times from each
    //   interval are produced in the merged result.
}
