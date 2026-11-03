use super::*;
#[test]
fn invocation_outcomes_preserve_remote_rejection_and_unknown_execution() {
    assert_eq!(
        outcome(Err(CallError::OutcomeUnknown)),
        (Code::OutcomeUnknown, vec![])
    );
    assert_eq!(
        outcome(Err(CallError::Remote(Failure::Rejected(vec![0, 255])))),
        (Code::Rejected, vec![0, 255])
    );
    assert_eq!(
        outcome(Err(CallError::Remote(Failure::Closing))).0,
        Code::RemoteClosing
    );
    assert_eq!(outcome(Err(CallError::UnknownTarget)).0, Code::UnknownTarget);
    assert_eq!(
        outcome(Err(CallError::AmbiguousTarget)).0,
        Code::AmbiguousTarget
    );
}
