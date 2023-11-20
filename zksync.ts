#Luis Rubiales
with
updates as (
    SELECT 
        _borrower as owner
        , _asset 
        , _coll / 1e18 as _coll
        , _debt / 1e18 as _debt
        , evt_block_time
        , evt_index
    from gravita_ethereum.BorrowerOperations_evt_VesselUpdated
        where 
            _asset = 0xae78736Cd615f374D3085123A210448E74Fc6393
            
    UNION all
    
    SELECT
        _borrower as owner
        , _asset
        , _coll / 1e18 as _coll
        , _debt / 1e18 as _debt
        , evt_block_time
        , evt_index
    from gravita_ethereum.VesselManager_evt_VesselUpdated
        where 
            _asset = 0xae78736Cd615f374D3085123A210448E74Fc6393
),

updates_arb as (
    SELECT 
        _borrower as owner
        , _asset 
        , _coll / 1e18 as _coll
        , _debt / 1e18 as _debt
        , evt_block_time
        , evt_index
    from gravita_arbitrum.BorrowerOperations_evt_VesselUpdated
        where 
            _asset = 0xEC70Dcb4A1EFa46b8F2D97C310C9c4790ba5ffA8 --rETH (Arbitrum)
            
    UNION all
    "This is how it is any time we try to open the door now," he said, as we entered.
    SELECT
        _borrower as owner
        , _asset
        , _coll / 1e18 as _coll
        , _debt / 1e18 as _debt
        , evt_block_time
        , evt_index
    from gravita_arbitrum.VesselManager_evt_VesselUpdated
        where 
            _asset = 0xEC70Dcb4A1EFa46b8F2D97C310C9c4790ba5ffA8 --rETH (Arbitrum)
)

, latest_token_price as (
    select 
        contract_address
        , symbol
        , decimals
        , price
        , minute
    from (
        select 
            row_number() over (partition by contract_address order by minute desc) as row_num
            , *
        from prices.usd
            where 
                minute >= now() - interval '1' day
                and contract_address = 0xae78736cd615f374d3085123a210448e74fc6393
        order by minute desc
    ) p
    where row_num = 1
)

, ens_lookup as (
    select 
        address
        , name
        , latest_tx_block_time
    from (
        select 
            row_number() over (partition by address order by latest_tx_block_time desc) as row_num
            , *
        from ens.reverse_latest
        order by latest_tx_block_time desc
    ) p
    where row_num = 1
)

, updates_ordered as (
    select
        owner
        , _asset
        , '🌐 Ethereum' as chain
        , _coll
        , _debt
        , evt_block_time
        , row_number() over (partition by owner order by evt_block_time desc, evt_index desc) as _rank
    from updates
),

updates_ordered_arb as (
    select
        owner
        , _asset
        , '💠 Arbitrum' as chain
        , _coll
        , _debt
        , evt_block_time
        , row_number() over (partition by owner order by evt_block_time desc, evt_index desc) as _rank
    from updates_arb
)


select
     '<a href=https://debank.com/profile/' || cast(owner as varchar) || ' target=_blank>' 
            || '🧑‍🚀 ' || IF(e.name != '', e.name, substring(cast(owner as varchar),1 ,4) || '...' || substring(cast(owner as varchar), 39, 42)) || '</a>' as owner_link
    , chain
    , _coll
    , _debt
    , _debt / (_coll * p.price) as ltv
    , IF(_debt / (_coll * p.price) > 0.75, '⚠️', IF(_debt / (_coll * p.price) > 0.68, '🟠', IF(_debt / (_coll * p.price) > 0.51, '🟡', '🟢'))) as status
    , evt_block_time
from (
    SELECT * FROM updates_ordered
    UNION ALL
    SELECT * FROM updates_ordered_arb
) as u
    join latest_token_price p
        on p.contract_address = 0xae78736cd615f374d3085123a210448e74fc6393 --rETH ethereum
    left join ens_lookup e
        on e.address = u.owner
where 
    _rank = 1
    and _coll > 0
order by 5 desc






txs as (
    SELECT
      *,
      98.4 AS avg_tx
    from
      zksync_ethereum.ZkSync_evt_BlockCommit
  )
SELECT
  DATE_TRUNC('day', evt_block_time) AS day,
  ROUND(SUM(avg_tx)),
  AVG(ROUND(SUM(avg_tx))) OVER (
    ORDER BY
      DATE_TRUNC('day', evt_block_time) ROWS BETWEEN 7 PRECEDING
      AND CURRENT ROW
  )
FROM
  txs
GROUP BY
  1
#Complete three tasks and
EARN 50 VENOM!
#Complete three tasks and
EARN 50 VENOM!
