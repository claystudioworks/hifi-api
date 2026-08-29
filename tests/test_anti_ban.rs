//! Anti-ban tests — per-account governor + EWMA

use hifi_api::account_manager::{AccountManager, SwitchingWeights};

#[tokio::test]
async fn test_per_account_limiter_blocks_after_burst() {
    let am = AccountManager::new(None, SwitchingWeights::default());
    am.add_account("t".into(), "id".into(), "sec".into(), "tok".into(), None)
        .await
        .unwrap();
    let acc = am.list_accounts().await[0].clone();
    // should allow 3 burst, 4th within 1 sec should be rate limited
    assert!(acc.can_consume());
    assert!(acc.can_consume());
    assert!(acc.can_consume());
    assert!(!acc.can_consume()); // 4th blocked
    // after 1 sec, token refills
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert!(acc.can_consume());
}

#[tokio::test]
async fn test_ewma_auto_disables_after_errors() {
    let am = AccountManager::new(None, SwitchingWeights::default());
    let acc = am
        .add_account("t2".into(), "id2".into(), "sec2".into(), "tok2".into(), None)
        .await
        .unwrap();
    assert!(acc.is_active.load(std::sync::atomic::Ordering::Relaxed));
    acc.record_ewma_error();
    assert!(acc.is_active.load(std::sync::atomic::Ordering::Relaxed)); // 300 not >300
    acc.record_ewma_error();
    // after 2 errors, EWMA 510 >300 should auto-disable
    assert!(!acc.is_active.load(std::sync::atomic::Ordering::Relaxed));
}

#[tokio::test]
async fn test_select_skips_rate_limited_account() {
    let am = AccountManager::new(None, SwitchingWeights::default());
    let acc1 = am
        .add_account("a1".into(), "id1".into(), "sec1".into(), "tok1".into(), None)
        .await
        .unwrap();
    let _acc2 = am
        .add_account("a2".into(), "id2".into(), "sec2".into(), "tok2".into(), None)
        .await
        .unwrap();
    // exhaust acc1's limiter (3 burst)
    assert!(acc1.can_consume());
    assert!(acc1.can_consume());
    assert!(acc1.can_consume());
    assert!(!acc1.can_consume());
    // select should skip acc1 and pick acc2 (which still has tokens)
    let selected = am.select_account().await.unwrap();
    assert_eq!(selected.id, _acc2.id);
}
