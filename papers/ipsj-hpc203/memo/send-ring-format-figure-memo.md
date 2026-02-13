# send ring 図作成メモ

## 目的
- 現在の論文中の「send ringのメモリフォーマット表」は、以下2種類の情報が混在している。
1. 個々のメッセージ（スロット）構造
2. マルチプロセス同時書き込み向けの2-phase公開手順
- 図ではこの2つを分離して示す。

## 図を2枚に分ける案

### 図A: スロット単位メモリフォーマット（静的構造）
- タイトル案: `send ring slot format`
- 1スロットを左から右へ配置:
1. `Slot Header`
2. `Payload Area`
3. `Padding/Align`
- `Slot Header` のフィールド例:
1. `call_id`
2. `generation`
3. `state (Empty / ReqReady / RespReady)`
4. `req_len`
5. `resp_len`
- 併記するグローバル管理情報（リング全体）:
1. `producer_pos`
2. `consumer_pos`

注記:
- 「要求と応答は同一send ring上の同一スロットを使う」
- 「local完結要求は別IPCキューなのでこの図の対象外」

### 図B: 2-phase公開プロトコル（動的手順）
- タイトル案: `slot publish protocol (multi-process writers)`
- クライアント/デーモンの時系列シーケンスで描く:
1. `reserve`: クライアントが原子的tail更新で書き込み区間を確保
2. `write`: Header + Payloadを書き込み
3. `publish`: stateを`ReqReady`へ遷移（ここで可視化）
4. `observe`: デーモンが`ReqReady`を検知
5. `emit`: デーモンがWQEを作成してdoorbell
6. `complete`: 応答受信後にデーモンが同スロットへ書き戻し，`RespReady`
7. `consume`: クライアントが応答を消費し`Empty`へ戻す

注記:
- デーモンはペイロードを解釈せず、境界情報と状態のみ参照
- race防止の本質は「reserve」と「publish」の分離

## 論文本文との対応
- 図Aは `RDMA通信プロトコル / 転送と通知` と `sendバッファ論理フォーマット表` の代替または補助
- 図Bは `RDMA通信プロトコル / 送信バッファ書き込み調停` の説明図

## 置換時メモ
- 既存の表は残すなら簡略化する（フィールド名のみ）
- 図を主にするなら表を削除し、「詳細は図X, Y」とする
