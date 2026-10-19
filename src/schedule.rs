/// Daily window in UTC minutes after midnight. Equal bounds mean all day.
/// Fields are private so callers must use the range-validating constructor.
///
/// ```compile_fail
/// use independent_download_manager::schedule::DailyWindow;
/// let invalid = DailyWindow { starts_at_minute: 1440, ends_at_minute: 1440 };
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DailyWindow {
    starts_at_minute: u16,
    ends_at_minute: u16,
}

impl DailyWindow {
    pub fn new(starts_at_minute: u16, ends_at_minute: u16) -> Option<Self> {
        (starts_at_minute < 1440 && ends_at_minute < 1440).then_some(Self {
            starts_at_minute,
            ends_at_minute,
        })
    }

    pub fn starts_at_minute(self) -> u16 {
        self.starts_at_minute
    }

    pub fn ends_at_minute(self) -> u16 {
        self.ends_at_minute
    }

    pub fn contains_minute(self, minute: u16) -> bool {
        if minute >= 1440 {
            return false;
        }
        if self.starts_at_minute == self.ends_at_minute {
            return true;
        }
        if self.starts_at_minute < self.ends_at_minute {
            (self.starts_at_minute..self.ends_at_minute).contains(&minute)
        } else {
            minute >= self.starts_at_minute || minute < self.ends_at_minute
        }
    }

    /// Parses a daily window from explicit 24-hour UTC `HH:MM` values.
    pub fn parse_utc_range(starts_at: &str, ends_at: &str) -> Option<Self> {
        Self::new(parse_utc_hhmm(starts_at)?, parse_utc_hhmm(ends_at)?)
    }
}

fn parse_utc_hhmm(value: &str) -> Option<u16> {
    let bytes = value.as_bytes();
    if bytes.len() != 5
        || bytes[2] != b':'
        || !bytes[..2].iter().all(u8::is_ascii_digit)
        || !bytes[3..].iter().all(u8::is_ascii_digit)
    {
        return None;
    }
    let hours = u16::from(value[0..2].parse::<u8>().ok()?);
    let minutes = u16::from(value[3..5].parse::<u8>().ok()?);
    if hours > 23 || minutes > 59 {
        return None;
    }
    Some(hours * 60 + minutes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daily_window_supports_overnight_ranges_and_excludes_the_end() {
        let window = DailyWindow::new(22 * 60, 6 * 60).unwrap();
        assert!(window.contains_minute(23 * 60));
        assert!(window.contains_minute(5 * 60));
        assert!(!window.contains_minute(6 * 60));
        assert!(!window.contains_minute(12 * 60));
        assert!(DailyWindow::new(1440, 0).is_none());
    }

    #[test]
    fn public_constructor_rejects_out_of_range_bounds_and_keeps_equal_bounds_valid() {
        assert!(DailyWindow::new(0, 1440).is_none());
        let all_day = DailyWindow::new(1439, 1439).unwrap();
        assert_eq!(all_day.starts_at_minute(), 1439);
        assert_eq!(all_day.ends_at_minute(), 1439);
        assert!(all_day.contains_minute(0));
        assert!(all_day.contains_minute(1439));
    }

    #[test]
    fn utc_window_parser_requires_valid_explicit_hhmm_values() {
        assert_eq!(
            DailyWindow::parse_utc_range("22:15", "06:30"),
            DailyWindow::new(22 * 60 + 15, 6 * 60 + 30)
        );
        assert!(DailyWindow::parse_utc_range("9:00", "10:00").is_none());
        assert!(DailyWindow::parse_utc_range("24:00", "10:00").is_none());
        assert!(DailyWindow::parse_utc_range("10:00Z", "11:00").is_none());
    }
}
