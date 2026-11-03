use super::*;

#[test]
fn accepted_uri_preserves_raw_path_query_and_port() {
    let frame = Frame {
        scheme: "https".into(),
        raw_path: b"/a%2Fb".to_vec(),
        query: b"x=1%202".to_vec(),
        headers: vec![(b"host".to_vec(), b"example.test:8443".to_vec())],
        ..Default::default()
    };
    assert_eq!(
        &*absolute_uri(&frame).unwrap(),
        "https://example.test:8443/a%2Fb?x=1%202"
    );
}

#[test]
fn missing_authority_and_scheme_are_not_invented() {
    assert!(matches!(
        absolute_uri(&Frame::default()),
        Err(Status::InvalidArgument)
    ));
}

#[test]
fn output_requires_one_head_and_one_pending_operation() {
    let mut gate = OutputGate { phase: Phase::New, busy: false };
    assert_eq!(gate.reserve(Kind::Write), Err(Status::AlreadyCompleted));
    gate.reserve(Kind::Head).unwrap();
    assert_eq!(gate.reserve(Kind::Write), Err(Status::Busy));
    gate.busy = false;
    assert_eq!(gate.reserve(Kind::Head), Err(Status::AlreadyCompleted));
    gate.reserve(Kind::Write).unwrap();
    assert_eq!(gate.reserve(Kind::Finish), Err(Status::Busy));
    gate.busy = false;
    gate.reserve(Kind::Finish).unwrap();
    gate.busy = false;
    assert_eq!(gate.reserve(Kind::Write), Err(Status::AlreadyCompleted));
    gate.phase = Phase::Aborted;
    assert_eq!(gate.reserve(Kind::Write), Err(Status::Cancelled));
}
