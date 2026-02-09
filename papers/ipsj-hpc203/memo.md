# 目標

- ノード内リクエスト集約により、ノード間通信においてQueue depthを生かしつつConnectionを減らして
負荷減少しつつレイテンシ隠蔽で高速化
- ad-hoc filesystem, kvsなどを前提として、HPCアプリにおいて補助的に展開されるミドルウェアを想定
- 大規模ノードでのscalabilityを確保する

ストーリーとしては、Fat nodeにおける通信爆発を解決するためにノード内通信をやる。
丁寧なipc/inprocの実装選択と、CPU間転送の考察によって最適化する。
その上で、IPCとHITMといったキャッシュ関連のperfパラメータを手掛かりにアクセスパターンを最適化する。
RDMA NICは十分に高速なので、NICの基本（DCでAV切り替えると性能が遅くなる。これはDCは高速接続するRCに近いから。UDでは起きないが、代わりにone-sided通信が出来ない）などを踏まえ、NIC friendlyな設計を意識すれば、少数コアでのNIC操作における主要なボトルネックは結局CPUがどれだけ効率的にメモリ操作出来るかにかかってくる。なので、全てをメモリ操作の最適化として捉えて高速化を図る。接続数はad-hoc filesystemみたいなワークロードに限れば、アーキテクチャレベルでの改善の方が簡単だしNICの性能を活かしやすい。

## 構成要素

- ipc
  - client/daemon通信を行う
  - 低Queue depthでclient多数状況への最適化
  - client/daemon共有メモリとそのアドレス変換をサポートする
  - 性能評価: 低QD、多数clientのベンチマークによって性能評価。多数のSOTA実装との比較
- inproc
  - daemon内delegation
  - 性能評価: 中QD(8とか32くらい）、少数clientのall-to-allベンチマークによって性能評価。多数のSOTA実装との比較
- copyrpc
  - daemon/daemonのノード間通信を行う
  - write with immediateでのバッファ転送が主体
  - end-to-endのソフトウェア flow controlでハングを起こさない
  - バッファリングと一括転送により、SQ/RQのWQE消費を減らして、WQE枯渇によるRNR retryを回避
  - memory pollingに対して、CQへのイベント集約による多数QPでの効率化を狙う
  - UCXとの比較、naive send/recvとの比較。（間に合えば memory pollingとの比較）
- benchkv
  - rank + keyによるバチクソ単純なKVS。これ自体はマジで軽いのでアーキテクチャだけ評価

## 評価項目

- mpsc性能評価
  - spsc集約 v.s. mpsc
  - 低QD or 高QD
  - HITMは？どのくらいキャッシュコヒーレンシプロトコルが走ってどのくらい他の操作に影響する？
  - Store bufferの節約は出来た方がいいよね
- RPC性能評価
  - raw send recvとUCXとcopyrpcで勝負
- 統合評価
  - IPCとかのキャッシュ関連パラメータとIOPSの評価
  - YCSB like benchmark
  - ここの評価を逆算して最適化

## その他小噺（実装部分とかに記載かな）

- mlx5デバイス使うならibverbsやめた方がいいよ。直接メモリさわればメモリアクセスを最適化出来るし、オーバーヘッドも減る
- 実はキャッシュコヒーレンシプロトコルでのデータ転送よりもCLDEMOTEでL3に落としてそれを他のコアで読む方がレイテンシ低かったりするよ
- QP数による性能低下はConnect-X7とかだとまた話が変わってきているよ。別に1000個くらいなら割と動くよ
