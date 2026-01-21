use kubera_core::{EventBus, connector::MockConnector, connector::MarketConnector};
use std::sync::Arc;

#[tokio::test]
async fn test_mock_connector_integration() {
    let bus = EventBus::new(100);
    let connector = MockConnector::new("TestMock");
    
    assert_eq!(connector.name(), "TestMock");
    
    // Test that it runs and stops without error
    let result = connector.run().await;
    assert!(result.is_ok());
    
    connector.stop();
}
