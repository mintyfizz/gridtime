# 002 – Silver: long format, change-only versions, watermark in snapshot properties

**Status:** accepted · 2026-10-15

**Context.** Elia publishes the same minute several times: the near-real-time feed (ods175)
refreshes values during the current hours, and the historical feed (ods165) republishes them
the next day. The poller re-fetches overlapping windows every minute, so Bronze holds many
copies of identical records. Some fields are named differently between the two feeds, and the
live data showed that the historical feed is not fully validated: Elia flags many of its rows
`NotValidated` or `DataIssue`.

**Decision.**
1. **Long format.** One Silver row per (minute, measure, version), with canonical measure names
   (`transform/measures.py`). New Elia fields become new mapping entries, not schema changes.
2. **Change-only versions.** A row is written only when the value, Elia's quality flag or the
   publication stage changed since the previous version of that (minute, measure). A historical
   republication with an identical value is still recorded, because it is a confirmation.
3. **Stage, not "validated".** Each row records its publication `stage`
   (`near_real_time` / `historical`) and Elia's own `quality_status`. We do not claim validation
   Elia has not given.
4. **Watermark in Iceberg snapshot properties.** Each Silver commit stores the highest Bronze
   `transaction_time` it processed (`gridtime.bronze-watermark`). The job needs no state store,
   and the watermark is versioned together with the data it describes.

**Assumptions to verify.**
- `mfrrup`/`upwardavailablemfrrprice`, `mfrrdown`/`downwardavailablemfrrprice` and the inter-TSO
  price pairs are the same quantities under different names. The revision statistics will show
  systematic gaps if they are not.
- Bronze has a single sequential writer, so rows never arrive with a `transaction_time` older
  than the watermark. Concurrent ingestion (milestone 3+) must keep this or switch the watermark
  to Bronze snapshot IDs.

**Consequences.** "Versions per minute" counts real revisions; as-of queries are a single window
function; Silver is much smaller than Bronze. Cost: change detection must read the latest stored
version of every affected minute on each run.
