{{
  config(
    materialized = 'incremental',
    labels = {'type': 'stripe', 'contains_pie': 'yes', 'category':'source'}  
   )
}}

with daily_balance_transactions as (
  select

    case when type = 'payout' then cast(TIMESTAMP_SECONDS(available_on) as date) else cast(created as date) end as day,
    currency,
    sum(net) as daily_balance,
    sum(case when type = 'payout' then net else 0 end) as payouts,
    sum(case when type != 'payout' then net else 0 end) as net_transactions,
    sum(case when type in ('charge', 'payment') then net else 0 end) as payments, -- net = amount - fee
    sum(case when type in ('payment_refund', 'refund', 'payment_failure_refund') then net else 0 end) as refunds,
    sum(case when type = 'transfer' then net else 0 end) as transfers,
    sum(case when type = 'adjustment' and lower(description) like 'chargeback withdrawal%' then net else 0 end) as chargeback_withdrawals,
    sum(case when type = 'adjustment' and lower(description) like 'chargeback reversal%' then net else 0 end) as chargeback_reversals,
    sum(case when type = 'adjustment' and lower(description) not like 'chargeback withdrawal%' and lower(description) not like 'chargeback reversal%' then net else 0 end) as other_adjustments,
    sum(case when type not in ('payout', 'transfer', 'charge', 'payment', 'refund', 'payment_refund', 'adjustment') then net else 0 end) as other_transactions
  from  {{ source('stripe',  'balance_transactions') }}
  group by 1, 2
) 

-- Compute the current_balance for each day and format output
select
  cast(day as date) as date ,
  currency,
  -- use SUM Window Function to calc. running total
  sum(daily_balance) over(partition by currency order by day)/100.0 as current_balance,
  payouts/100.0 as payouts,
  net_transactions/100.0 as net_transactions,
  payments/100.0 as payments,
  refunds/100.0 as refunds,
  transfers/100.0 as transfers,
  chargeback_withdrawals/100.0 as chargeback_withdrawals,
  chargeback_reversals/100.0 as chargeback_reversals,
  other_adjustments/100.0 as other_adjustments,
  other_transactions/100.0 as other_transactions
from daily_balance_transactions
order by 1 desc, 2