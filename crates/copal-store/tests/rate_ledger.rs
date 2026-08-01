//! The shared ledger's arithmetic, against a real engine: guarded
//! increments admit within the budget, refuse past it, and spend
//! nothing on refusal.

use copal_store::repo::rate;
use copal_store::{Store, StoreConfig};

#[tokio::test]
async fn charges_admit_refuse_and_spend_nothing_on_refusal() {
    let store = Store::connect(StoreConfig::memory()).await.unwrap();
    let key = rate::window_key("reads:key-01", 12345).unwrap();
    rate::ensure_window(&store, &key, 12345).await.unwrap();
    // Racing creators collide harmlessly.
    rate::ensure_window(&store, &key, 12345).await.unwrap();

    assert!(rate::try_charge(&store, &key, 6, 10).await.unwrap());
    assert!(rate::try_charge(&store, &key, 4, 10).await.unwrap());
    // Exactly spent: the refusal records nothing, so a fitting charge
    // afterwards still passes.
    assert!(!rate::try_charge(&store, &key, 1, 10).await.unwrap());
    assert!(rate::try_charge(&store, &key, 0, 10).await.unwrap());

    // Another bucket and another minute have their own ledgers.
    let other = rate::window_key("reads:key-02", 12345).unwrap();
    rate::ensure_window(&store, &other, 12345).await.unwrap();
    assert!(rate::try_charge(&store, &other, 10, 10).await.unwrap());
    let next = rate::window_key("reads:key-01", 12346).unwrap();
    rate::ensure_window(&store, &next, 12346).await.unwrap();
    assert!(rate::try_charge(&store, &next, 10, 10).await.unwrap());

    // The sweep drops finished minutes and keeps the current one.
    let dropped = rate::cleanup_windows(&store, 12346).await.unwrap();
    assert_eq!(dropped, 2, "both windows of minute 12345");
    assert!(rate::try_charge(&store, &next, 0, 10).await.unwrap());
}
