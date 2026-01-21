use kubera_core::{connector::MockConnector, connector::MarketConnector};

#[tokio::test]
async fn test_mock_connector_integration() {
    let _bus = kubera_core::EventBus::new(100);
    let connector = MockConnector::new("TestMock");
    
    assert_eq!(connector.name(), "TestMock");
    
    // Test that it runs and stops without error
    let result = connector.run().await;
    assert!(result.is_ok());
    
    connector.stop();
}
