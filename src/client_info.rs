use crate::binary::Encoder;

pub static CLIENT_NAME: &str = "Rust SQLDriver";

pub const CLICK_HOUSE_REVISION: u64 = 54429; // DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS
pub const CLICK_HOUSE_DBMSVERSION_MAJOR: u64 = 1;
pub const CLICK_HOUSE_DBMSVERSION_MINOR: u64 = 1;

pub fn write(encoder: &mut Encoder) {
    encoder.string(CLIENT_NAME);
    encoder.uvarint(CLICK_HOUSE_DBMSVERSION_MAJOR);
    encoder.uvarint(CLICK_HOUSE_DBMSVERSION_MINOR);
    encoder.uvarint(CLICK_HOUSE_REVISION);
}

pub fn description() -> String {
    format!(
        "{CLIENT_NAME} {CLICK_HOUSE_DBMSVERSION_MAJOR}.{CLICK_HOUSE_DBMSVERSION_MINOR}.{CLICK_HOUSE_REVISION}",
    )
}

#[test]
fn test_description() {
    assert_eq!(
        description(),
        format!(
            "Rust SQLDriver {}.{}.{}",
            CLICK_HOUSE_DBMSVERSION_MAJOR, CLICK_HOUSE_DBMSVERSION_MINOR, CLICK_HOUSE_REVISION
        )
    )
}
