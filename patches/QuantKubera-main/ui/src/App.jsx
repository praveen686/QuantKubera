import React, { useState, useEffect, useRef } from 'react';
import { Activity, Shield, TrendingUp, Terminal, Play, Square } from 'lucide-react';

function App() {
  const [status, setStatus] = useState({ equity: 100000.0, realized_pnl: 0.0 });
  const [symbols, setSymbols] = useState({});
  const [logs, setLogs] = useState([]);
  const [connected, setConnected] = useState(false);
  const ws = useRef(null);

  useEffect(() => {
    connect();
    return () => ws.current?.close();
  }, []);

  const connect = () => {
    ws.current = new WebSocket('ws://localhost:8080/ws');

    ws.current.onopen = () => {
      setConnected(true);
      console.log('Connected to QuantKubera Server');
    };

    ws.current.onmessage = (event) => {
      const msg = JSON.parse(event.data);
      if (msg.type === 'Status') {
        setStatus(msg.payload);
      } else if (msg.type === 'SymbolUpdate') {
        setSymbols(prev => ({
          ...prev,
          [msg.payload.symbol]: msg.payload
        }));
      } else if (msg.type === 'OrderLog') {
        setLogs(prev => [msg.payload.message, ...prev.slice(0, 49)]);
      }
    };

    ws.current.onclose = () => {
      setConnected(false);
      setTimeout(connect, 3000);
    };
  };

  const sendCommand = (action, target) => {
    if (ws.current?.readyState === WebSocket.OPEN) {
      ws.current.send(JSON.stringify({
        type: 'Command',
        payload: { action, target }
      }));
    }
  };

  return (
    <div className="dashboard">
      <header>
        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
          <Shield size={32} color="#58a6ff" />
          <h1 style={{ fontSize: '1.5rem' }}>QuantKubera Control Plane</h1>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
          <div style={{ color: connected ? '#3fb950' : '#f85149', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <Activity size={16} />
            {connected ? 'LIVE' : 'DISCONNECTED'}
          </div>
        </div>
      </header>

      <aside className="sidebar">
        <div className="nav-group">
          <h3>Subsystems</h3>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem', marginTop: '1rem' }}>
            <button className="btn btn-success" onClick={() => sendCommand('START', 'ALL')}>
              <Play size={16} style={{ marginRight: '8px' }} /> Enable All
            </button>
            <button className="btn btn-danger" onClick={() => sendCommand('STOP', 'ALL')}>
              <Square size={16} style={{ marginRight: '8px' }} /> Global Stop
            </button>
          </div>
        </div>
      </aside>

      <main>
        <div className="stats-grid">
          <div className="card">
            <h3>Net Equity</h3>
            <div className="value">${status.equity.toLocaleString(undefined, { minimumFractionDigits: 2 })}</div>
          </div>
          <div className="card">
            <h3>Realized PnL</h3>
            <div className={`value ${status.realized_pnl >= 0 ? 'positive' : 'negative'}`}>
              {status.realized_pnl >= 0 ? '+' : ''}${status.realized_pnl.toLocaleString(undefined, { minimumFractionDigits: 2 })}
            </div>
          </div>
          <div className="card">
            <h3>Active Symbols</h3>
            <div className="value">{Object.keys(symbols).length}</div>
          </div>
        </div>

        <div className="card">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
            <h3>Market Watch</h3>
            <TrendingUp size={20} color="#8b949e" />
          </div>
          <table className="symbol-table">
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Last Price</th>
                <th>Position</th>
                <th>Notional</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {Object.values(symbols).map(s => (
                <tr key={s.symbol}>
                  <td style={{ fontWeight: 600 }}>{s.symbol}</td>
                  <td className="accent-blue">${s.last_price.toFixed(2)}</td>
                  <td>{s.position.toFixed(4)}</td>
                  <td>${(s.position * s.last_price).toFixed(2)}</td>
                  <td>
                    {s.symbol === 'BTCUSDT' ? (
                      <span style={{ background: '#3fb950', color: '#0d1117', padding: '2px 8px', borderRadius: '4px', fontSize: '0.75rem', fontWeight: 800 }}>ORAS-ACTIVE</span>
                    ) : (
                      <span style={{ color: '#3fb950', fontSize: '0.8rem' }}>● Tracking</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="card" style={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
            <h3>System Logs</h3>
            <Terminal size={20} color="#8b949e" />
          </div>
          <div className="log-panel">
            {logs.map((log, i) => (
              <div key={i} className="log-entry">{log}</div>
            ))}
            {logs.length === 0 && <div className="log-entry">Waiting for system events...</div>}
          </div>
        </div>
      </main>
    </div>
  );
}

export default App;
