use super::*;
#[test]
fn stop_start_and_restart_retire_only_the_previous_generation() {
    let mut state = State::initial();
    let first = Arc::clone(&state.generation);
    assert_eq!(
        state.apply(Action::Start).unwrap(),
        Snapshot { generation: 1, running: 1 }
    );
    assert!(!first.cancelled.is_cancelled());
    assert_eq!(
        state.apply(Action::Stop).unwrap(),
        Snapshot { generation: 1, running: 0 }
    );
    assert!(first.cancelled.is_cancelled());
    assert_eq!(state.apply(Action::Stop).unwrap().generation, 1);
    assert_eq!(
        state.apply(Action::Start).unwrap(),
        Snapshot { generation: 2, running: 1 }
    );
    let second = Arc::clone(&state.generation);
    assert!(!second.cancelled.is_cancelled());
    assert_eq!(
        state.apply(Action::Restart).unwrap(),
        Snapshot { generation: 3, running: 1 }
    );
    assert!(second.cancelled.is_cancelled());
    assert!(!state.generation.cancelled.is_cancelled());
    state.apply(Action::Stop).unwrap();
    assert_eq!(state.apply(Action::Restart).unwrap().generation, 4);
}
#[test]
fn generation_identity_does_not_wrap_or_partially_stop() {
    let mut state = State::initial();
    state.generation = Arc::new(Generation {
        id: u64::MAX,
        cancelled: CancellationToken::new(),
    });
    assert_eq!(state.apply(Action::Restart), Err(Status::NoCapacity));
    assert!(!state.generation.cancelled.is_cancelled());
    assert!(state.running);
}
#[test]
fn control_notifications_are_bounded_without_deferring_retirement() {
    let lifecycle = Lifecycle::new(1);
    let token = lifecycle.token(1).unwrap().unwrap();
    lifecycle.apply(Action::Start).unwrap();
    assert_eq!(lifecycle.apply(Action::Stop), Err(Status::NoCapacity));
    assert!(token.is_cancelled());
    assert!(lifecycle.capture().unwrap().is_none());
}
