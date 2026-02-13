# 図表修正チェックリスト（重複・タイポ・表記揺れ）

対象: `hpc203.tex` で参照している以下の図
- `handmade-figures/pfs-and-adhocfs.drawio.pdf`
- `handmade-figures/RDMA-SQ-and-RQ.drawio.pdf`
- `handmade-figures/in-node-architecture.drawio.pdf`
- `handmade-figures/copyrpc.pdf`
- `handmade-figures/message-write.drawio.pdf`

## 1. 重複（番号/表現）

### `handmade-figures/message-write.drawio.pdf`
- 手順番号 `2.` が複数箇所に存在（手順追跡が困難）
  - 対応: 手順番号を一意に振り直し（1,2,3,4,5...）
- `generation` の記載が2箇所に分散して見える
  - 対応: 片方を「header書き込み」側に統合し，重複なら削除

## 2. タイポ

### `handmade-figures/RDMA-SQ-and-RQ.drawio.pdf`
- `MemoyRegion` -> `Memory Region`

### `handmade-figures/in-node-architecture.drawio.pdf`
- `send/revc bufffer` -> `send/recv buffer`

### `handmade-figures/copyrpc.pdf`
- `issue resuest` -> `issue request`

## 3. 表記揺れ

### 共通方針
- `Client` / `Daemon` は先頭大文字で統一
- `Req` / `Res` と `request` / `response` を混在させない
  - 推奨: 図内は短縮語 `Req` / `Resp` に統一
- `consume` / `consumed` / `consumer` の用途を統一
  - 位置ポインタは `consumer pos`
  - 状態名は `consumed`
- `slot header` の表記を統一
  - `slot header` か `Slot Header` のどちらかに揃える

### `handmade-figures/in-node-architecture.drawio.pdf`
- `Req/Response` と `Req/Res` が混在
  - 対応: `Req/Resp` へ統一
- `send buffer` / `recv buffer` はあるが，`slot` と `Buffer` の大文字小文字が混在
  - 対応: `slot`, `buffer` を小文字統一（見出しのみ大文字）

### `handmade-figures/message-write.drawio.pdf`
- `consume pos` と本文側の `consumer` が揺れている
  - 対応: `consumer pos` に統一
- `stateチェック` / `state可視化` の和英混在
  - 対応: `state check` / `publish state` などどちらかに統一

### `handmade-figures/copyrpc.pdf`
- `remain length` と `length` の関係が曖昧
  - 対応: `remaining length` に統一し，通常長は `length`

## 4. 優先順位（修正順）
1. タイポ修正（読者の違和感が強い）
2. 手順番号重複の解消（説明整合に直結）
3. 表記揺れ統一（全図の可読性向上）
