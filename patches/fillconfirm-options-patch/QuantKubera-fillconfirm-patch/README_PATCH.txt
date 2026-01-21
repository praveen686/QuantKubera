QuantKubera Patch: Zerodha Live Fill Confirmation (Options Executor)

What this patch does
- Adds broker-side fill confirmation for Zerodha option leg execution.
- Removes the "assume placed == filled" behavior in live mode for this path.
- Records avg fill price from Kite order history (/orders/{order_id}).
- Fail-safe: if a leg does not reach COMPLETE within 15s, cancels the leg, cancels prior legs, and aborts the multileg.

Files changed
- crates/kubera-options/src/execution.rs

Apply
From your QuantKubera repo root:
  unzip -o QuantKubera-fillconfirm-patch.zip

Run
- Use your usual Zerodha options runner.
- Watch logs for: "LIVE FILL CONFIRMATION" and leg status Filled/Rejected/Cancelled.
