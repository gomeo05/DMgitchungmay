txs as (
    SELECT
      *,
      98.4 AS avg_tx
    FROM
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
